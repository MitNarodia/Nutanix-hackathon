package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/MitNarodia/Nutanix-hackathon/internal/assembler"
	"github.com/MitNarodia/Nutanix-hackathon/internal/control"
	"github.com/MitNarodia/Nutanix-hackathon/internal/dataplane"
	"github.com/MitNarodia/Nutanix-hackathon/internal/discovery"
	"github.com/MitNarodia/Nutanix-hackathon/internal/scheduler"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
	pb "github.com/MitNarodia/Nutanix-hackathon/proto"
)

type Engine struct {
	NodeID    string
	Secret    []byte
	CAS       *store.CASBlobStore
	MetaStore *store.BoltMetaStore
	Client    *dataplane.Client
	BaseDir   string
	Tracker   *scheduler.HealthTracker

	// MarkQuiet, if set, is called with the path the assembler is about to
	// write so the watcher can ignore the fsnotify events it triggers.
	MarkQuiet func(path string)

	// Announce, if set, is called after a successful pull so the caller can
	// re-broadcast the file over gossip and let it propagate further.
	Announce func(fileID string, root [32]byte, lamportTS uint64)
}

func NewEngine(nodeID string, secret []byte, baseDir string, cas *store.CASBlobStore, meta *store.BoltMetaStore, client *dataplane.Client) *Engine {
	return &Engine{
		NodeID:    nodeID,
		Secret:    secret,
		CAS:       cas,
		MetaStore: meta,
		Client:    client,
		BaseDir:   baseDir,
		Tracker:   scheduler.NewHealthTracker(),
	}
}

// Pull syncs fileID from whichever of the given peers has it
func (e *Engine) Pull(ctx context.Context, fileID string, peers []discovery.PeerInfo) error {
	fmt.Printf("\nInitiating Sync for %s\n", filepath.Base(fileID))

	if len(peers) == 0 {
		return fmt.Errorf("no candidate peers available for %s", fileID)
	}

	grpcToHTTP := make(map[string]string, len(peers))
	grpcAddrs := make([]string, 0, len(peers))
	for _, p := range peers {
		g := p.GRPCAddr()
		grpcToHTTP[g] = p.HTTPAddr()
		grpcAddrs = append(grpcAddrs, g)
	}

	localMeta, localMetaErr := e.MetaStore.GetFileMeta(fileID)

	// Keep localRoot nil
	var localRoot []byte
	var knownChunks [][]byte
	if localMetaErr == nil {
		localRoot = localMeta.MerkleRoot[:]
		for _, h := range localMeta.ChunkHashes {
			hc := make([]byte, 32)
			copy(hc, h[:])
			knownChunks = append(knownChunks, hc)
		}
	}

	var diff *pb.DiffResponse
	var remoteMeta *store.FileMeta
	var lastErr error

	for _, grpcAddr := range grpcAddrs {
		ctrlClient, err := control.NewControlClient(grpcAddr, e.Secret, e.NodeID)
		if err != nil {
			lastErr = fmt.Errorf("control plane connection to %s failed: %w", grpcAddr, err)
			continue
		}

		d, err := ctrlClient.Diff(ctx, fileID, localRoot, knownChunks)
		ctrlClient.Close()
		if err != nil {
			lastErr = fmt.Errorf("diff with %s failed: %w", grpcAddr, err)
			continue
		}

		if len(d.RemoteMerkleRoot) == 0 {
			// peer doesn't have the file
			continue
		}

		httpPeer := grpcToHTTP[grpcAddr]

		rm, err := e.Client.FetchMeta(ctx, httpPeer, fileID)
		if err != nil {
			lastErr = fmt.Errorf("failed to fetch remote meta from %s: %w", httpPeer, err)
			continue
		}

		diff = d
		remoteMeta = rm
		break
	}

	if diff == nil || remoteMeta == nil {
		if lastErr != nil {
			return fmt.Errorf("no peer had file %s: %w", fileID, lastErr)
		}
		return fmt.Errorf("no peer had file %s", fileID)
	}

	if diff.IsSynced {
		fmt.Println("File is completely in sync.")
		return nil
	}

	fmt.Printf("Diff complete: %d chunks missing.\n", len(diff.MissingChunkHashes))

	rarityMap := scheduler.BuildRarityMap(ctx, diff.MissingChunkHashes, grpcAddrs, e.Secret, e.NodeID)

	var wg sync.WaitGroup
	errChan := make(chan error, len(rarityMap))

	for _, chunk := range rarityMap {
		if len(chunk.Peers) == 0 {
			return fmt.Errorf("chunk %x is unavailable in the cluster", chunk.Hash[:4])
		}

		wg.Add(1)
		go func(c scheduler.ChunkRarity) {
			defer wg.Done()
			var chunkHash [32]byte
			copy(chunkHash[:], c.Hash)

			for _, peerGrpc := range c.Peers {
				httpPeer, ok := grpcToHTTP[peerGrpc]
				if !ok || !e.Tracker.IsAvailable(httpPeer) {
					continue
				}

				for attempt := 1; attempt <= 3; attempt++ {
					data, err := e.Client.FetchChunk(httpPeer, chunkHash)
					if err != nil {
						if attempt == 3 {
							break
						}

						backoff := scheduler.CalcJitteredBackoff(attempt, 500*time.Millisecond, 5*time.Second)
						e.Tracker.MarkHot(httpPeer, backoff)
						time.Sleep(backoff)
						continue
					}

					e.CAS.Put(chunkHash, data)
					return
				}
			}
			errChan <- fmt.Errorf("exhausted all peers for chunk %x", c.Hash[:4])
		}(chunk)
	}

	wg.Wait()
	close(errChan)
	if len(errChan) > 0 {
		return <-errChan
	}
	fmt.Println("All chunks secured in CAS.")

	// write to the real relative path so this node can re-serve it to
	// other peers afterward, instead of just a one-hop copy
	outPath := filepath.Join(e.BaseDir, filepath.FromSlash(fileID))

	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory for %s: %w", outPath, err)
	}

	if e.MarkQuiet != nil {
		e.MarkQuiet(outPath)
	}

	sparse, err := assembler.NewSparseFile(outPath, int64(remoteMeta.SizeBytes), len(remoteMeta.ChunkHashes))
	if err != nil {
		return fmt.Errorf("assembler failed: %w", err)
	}

	var currentOffset int64 = 0
	for i, h := range remoteMeta.ChunkHashes {
		data, err := e.CAS.Get(h)
		if err != nil {
			sparse.Close()
			return fmt.Errorf("chunk %x missing from local CAS after transfer: %w", h[:4], err)
		}

		if err := sparse.WriteChunk(i, currentOffset, data); err != nil {
			sparse.Close()
			return fmt.Errorf("failed to write chunk %d of %s: %w", i, fileID, err)
		}

		currentOffset += int64(len(data))
	}

	if err := sparse.Close(); err != nil {
		return fmt.Errorf("failed to finalize %s: %w", outPath, err)
	}

	if err := e.MetaStore.PutFileMeta(*remoteMeta); err != nil {
		return fmt.Errorf("failed to save meta: %w", err)
	}

	if e.Announce != nil {
		e.Announce(remoteMeta.FileID, remoteMeta.MerkleRoot, remoteMeta.LamportTS)
	}

	fmt.Printf("File assembled successfully at: %s\n", outPath)
	return nil
}
