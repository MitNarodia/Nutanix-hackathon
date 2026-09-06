package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/MitNarodia/Nutanix-hackathon/internal/chunker"
	"github.com/MitNarodia/Nutanix-hackathon/internal/merkle"
	"github.com/MitNarodia/Nutanix-hackathon/internal/orchestrator"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

type SyncRequest struct {
	FileID   string   `json:"file_id"`
	SeedAddr string   `json:"seed_addr"`
	Peers    []string `json:"peers"`
}

func (d *Daemon) StartLocalAPI(port int, secret []byte) {
	mux := http.NewServeMux()

	engine := orchestrator.NewEngine(
		d.Name,
		secret,
		d.BaseDir,
		d.CAS,
		d.MetaStore,
		d.Client,
	)

	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("path")

		c := chunker.NewFastCDCChunker(8192)
		chunks, err := c.ChunkFile(path)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		var hashes [][32]byte

		for _, ch := range chunks {
			d.CAS.Put(ch.Hash, ch.Data)
			hashes = append(hashes, ch.Hash)
		}

		meta := store.FileMeta{
			FileID:      path,
			MerkleRoot:  merkle.ComputeRoot(chunks),
			ChunkHashes: hashes,
		}

		d.MetaStore.PutFileMeta(meta)

		fmt.Fprintf(
			w,
			"Done!!... Ingested %s (Root: %x)\n",
			path,
			meta.MerkleRoot[:4],
		)
	})

	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req SyncRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(req.Peers) == 0 {
			req.Peers = []string{req.SeedAddr}
		}

		go func() {
			bgCtx := context.Background()
			if err := engine.Pull(
				bgCtx,
				req.FileID,
				req.SeedAddr,
				req.Peers,
			); err != nil {
				log.Printf("\n❌ [%s] Sync failed: %v", d.Name, err)
			}
		}()

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "Sync initiated for %s\n", req.FileID)
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	fmt.Printf("🔌 Local CLI API listening on %s\n", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("API server exited: %v", err)
	}
}