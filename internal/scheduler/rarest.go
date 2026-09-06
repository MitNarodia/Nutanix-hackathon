package scheduler

import (
	"context"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/MitNarodia/Nutanix-hackathon/internal/control"
)

type ChunkRarity struct {
	Hash  []byte
	Peers []string
}

func BuildRarityMap(ctx context.Context, missingChunks [][]byte, peerAddrs []string, secret []byte, nodeID string) []ChunkRarity {
	rarityMap := make(map[string][]string)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, addr := range peerAddrs {
		wg.Add(1)

		go func(peerAddr string) {
			defer wg.Done()

			client, err := control.NewControlClient(peerAddr, secret, nodeID)
			if err != nil {
				return
			}
			defer client.Close()

			reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			available, err := client.CheckAvailability(reqCtx, missingChunks)
			if err != nil {
				return
			}

			mu.Lock()

			for _, hash := range available {
				key := hex.EncodeToString(hash)
				rarityMap[key] = append(rarityMap[key], peerAddr)
			}

			mu.Unlock()
		}(addr)
	}

	wg.Wait()

	var result []ChunkRarity

	for _, hash := range missingChunks {
		key := hex.EncodeToString(hash)

		result = append(result, ChunkRarity{
			Hash:  hash,
			Peers: rarityMap[key],
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return len(result[i].Peers) < len(result[j].Peers)
	})

	return result
}