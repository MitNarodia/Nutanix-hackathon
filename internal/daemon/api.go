package daemon

import (
	"context"
	"fmt"
	"net/http"

	"github.com/MitNarodia/Nutanix-hackathon/internal/assembler"
	"github.com/MitNarodia/Nutanix-hackathon/internal/chunker"
	"github.com/MitNarodia/Nutanix-hackathon/internal/merkle"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

func (d *Daemon) StartLocalAPI(port int) {
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
		peer := r.URL.Query().Get("peer")
		fileID := r.URL.Query().Get("id")

		if peer == "" || fileID == "" {
			http.Error(
				w,
				"Missing 'peer' or 'id' parameters",
				http.StatusBadRequest,
			)
			return
		}

		fmt.Printf("Starting sync for %s from %s...\n", fileID, peer)

		ctx := context.Background()

		meta, err := d.Client.FetchMeta(ctx, peer, fileID)
		if err != nil {
			http.Error(
				w,
				fmt.Sprintf("Meta sync failed: %v", err),
				http.StatusInternalServerError,
			)
			return
		}

		fmt.Printf(
			"   -> Blueprint received. Merkle Root: %x\n",
			meta.MerkleRoot[:4],
		)

		for i, hash := range meta.ChunkHashes {
			err := d.Client.FetchSingle(ctx, peer, hash)
			if err != nil {
				msg := fmt.Sprintf("Failed to fetch chunk %d: %v", i, err)
				fmt.Println("  Wrong Chunk !! " + msg)
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
		}

		fmt.Printf(
			"   -> %d chunks synced and cryptographically verified.\n",
			len(meta.ChunkHashes),
		)

		outPath := "restored_file.txt"

		if err := assembler.Assemble(
			d.CAS,
			meta.ChunkHashes,
			outPath,
		); err != nil {
			http.Error(
				w,
				fmt.Sprintf("Assembly failed: %v", err),
				http.StatusInternalServerError,
			)
			return
		}

		d.MetaStore.PutFileMeta(*meta)

		successMsg := fmt.Sprintf(
			"Done!! ... Sync complete! File assembled at %s\n",
			outPath,
		)

		fmt.Print(successMsg)
		fmt.Fprint(w, successMsg)
	})

	fmt.Printf(
		"Local CLI API listening on 127.0.0.1:%d\n",
		port,
	)

	http.ListenAndServe(
		fmt.Sprintf("127.0.0.1:%d", port),
		mux,
	)
}