package orchestrator

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/MitNarodia/Nutanix-hackathon/internal/assembler"
	"github.com/MitNarodia/Nutanix-hackathon/internal/control"
	"github.com/MitNarodia/Nutanix-hackathon/internal/dataplane"
	"github.com/MitNarodia/Nutanix-hackathon/internal/scheduler"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

type Engine struct {
	NodeID    string
	Secret    []byte
	CAS       *store.CASBlobStore
	MetaStore *store.BoltMetaStore
	Client    *dataplane.Client
	BaseDir   string
	Tracker   *scheduler.HealthTracker
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

func (e *Engine) Pull(ctx context.Context, fileID string, seedGrpcAddr string, allPeers []string) error {
	fmt.Printf("\nInitiating Sync for %s\n", filepath.Base(fileID))

	ctrlClient, err := control.NewControlClient(seedGrpcAddr, e.Secret, e.NodeID)
	if err != nil {
		return fmt.Errorf("control plane connection failed: %w", err)
	}
	defer ctrlClient.Close()

	localMeta, _ := e.MetaStore.GetFileMeta(fileID)
	var knownChunks [][]byte
	for _, h := range localMeta.ChunkHashes {
		hc := make([]byte, 32)
		copy(hc, h[:])
		knownChunks = append(knownChunks, hc)
	}

	host, portStr, _ := net.SplitHostPort(seedGrpcAddr)
	port, _ := strconv.Atoi(portStr)
	httpPeer := fmt.Sprintf("%s:%d", host, port+100)

	remoteMeta, err := e.Client.FetchMeta(ctx, httpPeer, fileID)
	if err != nil {
		return fmt.Errorf("failed to fetch remote meta: %w", err)
	}

	diff, err := ctrlClient.Diff(ctx, fileID, localMeta.MerkleRoot[:], knownChunks)
	if err != nil {
		return fmt.Errorf("diff failed: %w", err)
	}

	if diff.IsSynced {
		fmt.Println("File is completely in sync.")
		return nil
	}
	
	fmt.Printf("Diff complete: %d chunks missing.\n", len(diff.MissingChunkHashes))

	rarityMap := scheduler.BuildRarityMap(ctx, diff.MissingChunkHashes, allPeers, e.Secret, e.NodeID)

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
				host, portStr, _ := net.SplitHostPort(peerGrpc)
				port, _ := strconv.Atoi(portStr)
				httpPeer := fmt.Sprintf("%s:%d", host, port+100) 

				if !e.Tracker.IsAvailable(httpPeer) {
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

	outPath := filepath.Join(e.BaseDir, "restored_"+filepath.Base(fileID))
	sparse, err := assembler.NewSparseFile(outPath, 0, len(remoteMeta.ChunkHashes))
	if err != nil {
		return fmt.Errorf("assembler failed: %w", err)
	}

	var currentOffset int64 = 0
	for i, h := range remoteMeta.ChunkHashes {
		data, _ := e.CAS.Get(h)
		
		sparse.WriteChunk(i, currentOffset, data)
		currentOffset += int64(len(data))
	}
	
	sparse.Close()

	if err := e.MetaStore.PutFileMeta(*remoteMeta); err != nil {
		return fmt.Errorf("failed to save meta: %w", err)
	}

	fmt.Printf("File assembled successfully at: %s\n", outPath)
	return nil
}