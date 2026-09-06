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

// Pull executes the v3 Sync spec: gRPC diff -> Rarest-First -> HTTP fetch -> Sparse Assembly
func (e *Engine) Pull(ctx context.Context, fileID string, seedGrpcAddr string, allPeers []string) error {
	fmt.Printf("\n🚀 Initiating Sync for %s\n", filepath.Base(fileID))

	// 1. gRPC Control Plane - Merkle Diffing
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

	diff, err := ctrlClient.Diff(ctx, fileID, localMeta.MerkleRoot[:], knownChunks)
	if err != nil {
		return fmt.Errorf("diff failed: %w", err)
	}

	if diff.IsSynced {
		fmt.Println("✅ File is completely in sync.")
		return nil
	}
	
	fmt.Printf("🔍 Diff complete: %d chunks missing.\n", len(diff.MissingChunkHashes))

	// 2. Scheduler - Build Rarity Map
	rarityMap := scheduler.BuildRarityMap(ctx, diff.MissingChunkHashes, allPeers, e.Secret, e.NodeID)

	// 3. Data Plane - Concurrent Rarest-First Downloads
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
				// Infer HTTP Data Plane port from gRPC port (e.g. 9100 -> 9200)
				host, portStr, _ := net.SplitHostPort(peerGrpc)
				port, _ := strconv.Atoi(portStr)
				httpPeer := fmt.Sprintf("%s:%d", host, port+100) 

				// Apply Peer Health Tracking
				if !e.Tracker.IsAvailable(httpPeer) {
					continue
				}

				// Fetch with Jittered Backoff
				for attempt := 1; attempt <= 3; attempt++ {
					data, err := e.Client.FetchChunk(httpPeer, chunkHash)
					if err != nil {
						if attempt == 3 {
							break // Exhausted retries, try next peer
						}
						// Rate-limited or busy: backoff and retry
						backoff := scheduler.CalcJitteredBackoff(attempt, 500*time.Millisecond, 5*time.Second)
						e.Tracker.MarkHot(httpPeer, backoff)
						time.Sleep(backoff)
						continue
					}

					e.CAS.Put(chunkHash, data)
					return // Success
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
	fmt.Println("📦 All chunks secured in CAS.")

	// 4. Sparse File Assembly
	outPath := filepath.Join(e.BaseDir, "restored_"+filepath.Base(fileID))
	sparse, err := assembler.NewSparseFile(outPath, 0, len(diff.MissingChunkHashes))
	if err != nil {
		return fmt.Errorf("assembler failed: %w", err)
	}

	var currentOffset int64 = 0
	for i, h := range diff.MissingChunkHashes {
		var ch [32]byte
		copy(ch[:], h)
		data, _ := e.CAS.Get(ch)
		
		sparse.WriteChunk(i, currentOffset, data)
		currentOffset += int64(len(data))
	}
	
	sparse.Close()
	fmt.Printf("🎉 File assembled successfully at: %s\n", outPath)
	return nil
}