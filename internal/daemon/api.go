package daemon

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/MitNarodia/Nutanix-hackathon/internal/chunker"
	"github.com/MitNarodia/Nutanix-hackathon/internal/merkle"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

type SyncRequest struct {
	FileID string `json:"file_id"`
}

func (d *Daemon) StartLocalAPI(port int, secret []byte) {
	mux := http.NewServeMux()

	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("path")

		c := chunker.NewFastCDCChunker(8192)
		chunks, err := c.ChunkFile(path)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		var hashes [][32]byte
		var size uint64

		for _, ch := range chunks {
			d.CAS.Put(ch.Hash, ch.Data)
			hashes = append(hashes, ch.Hash)
			size += uint64(len(ch.Data))
		}

		ts := d.nextTS()
		root := merkle.ComputeRoot(chunks)

		meta := store.FileMeta{
			FileID:      path,
			MerkleRoot:  root,
			LamportTS:   ts,
			ChunkHashes: hashes,
			SizeBytes:   size,
		}

		d.MetaStore.PutFileMeta(meta)
		d.announce(path, root, ts)

		fmt.Fprintf(
			w,
			"Ingested %s (root: %x)\n",
			path,
			meta.MerkleRoot[:4],
		)
	})

	// /sync manually kicks off a sync for a file ID against whatever peers
	// gossip currently knows about; goes through the same dedup and
	// rarest-first path as an automatic sync.
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

		if req.FileID == "" {
			http.Error(w, "file_id is required", http.StatusBadRequest)
			return
		}

		d.triggerSync(req.FileID)

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "Sync initiated for %s\n", req.FileID)
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	fmt.Printf("Local CLI API listening on %s\n", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("API server exited: %v", err)
	}
}
