package dataplane

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/MitNarodia/Nutanix-hackathon/internal/auth"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
	"golang.org/x/sync/semaphore"
)

type Server struct {
	addr       string
	verifier   auth.Verifier
	cas        *store.CASBlobStore
	metaStore  *store.BoltMetaStore
	sem        *semaphore.Weighted
	maxStreams int64
}

func NewServer(addr string, verifier auth.Verifier, cas *store.CASBlobStore, metaStore *store.BoltMetaStore, maxConcurrentStreams int64) *Server {
	return &Server{
		addr:       addr,
		verifier:   verifier,
		cas:        cas,
		metaStore:  metaStore,
		sem:        semaphore.NewWeighted(maxConcurrentStreams),
		maxStreams: maxConcurrentStreams,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/chunk", s.handleGetChunk)
	mux.HandleFunc("/meta", s.handleGetMeta)

	server := &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	fmt.Printf(
		"Data plane server listening on %s (max concurrent: %d)\n",
		s.addr,
		s.maxStreams,
	)

	return server.ListenAndServe()
}

func (s *Server) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	if err := s.verifier.VerifyHTTP(r); err != nil {
		http.Error(
			w,
			fmt.Sprintf("Unauthorized: %v", err),
			http.StatusForbidden,
		)
		return
	}

	if !s.sem.TryAcquire(1) {
		w.Header().Set("Retry-After", "2")
		http.Error(
			w,
			"Too Many Requests - Server busy",
			http.StatusTooManyRequests,
		)
		return
	}
	defer s.sem.Release(1)

	hashHex := r.URL.Query().Get("h")
	if len(hashHex) != 64 {
		http.Error(w, "Invalid or missing chunk hash", http.StatusBadRequest)
		return
	}

	var hash [32]byte

	decoded, err := hex.DecodeString(hashHex)
	if err != nil || len(decoded) != 32 {
		http.Error(w, "Malformed chunk hash hex", http.StatusBadRequest)
		return
	}

	copy(hash[:], decoded)

	file, err := s.cas.GetFile(hash)
	if err != nil {
		http.Error(w, "Chunk not found", http.StatusNotFound)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, "", stat.ModTime(), file)
}

func (s *Server) handleGetMeta(w http.ResponseWriter, r *http.Request) {
	if err := s.verifier.VerifyHTTP(r); err != nil {
		http.Error(w, "Unauthorized", http.StatusForbidden)
		return
	}
	
	fileID := r.URL.Query().Get("id")
	meta, err := s.metaStore.GetFileMeta(fileID)
	if err != nil {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}