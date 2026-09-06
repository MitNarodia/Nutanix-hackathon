package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"io"
	"path/filepath"
)

type CASBlobStore struct {
	baseDir string
}

func NewCASBlobStore(baseDir string) (*CASBlobStore, error) {
	if err := os.MkdirAll(filepath.Join(baseDir, "objects"), 0755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Join(baseDir, "tmp"), 0755); err != nil {
		return nil, err
	}

	return &CASBlobStore{baseDir: baseDir}, nil
}

func (s *CASBlobStore) Put(hash [32]byte, data []byte) error {
	name := hex.EncodeToString(hash[:])
	path := filepath.Join(s.baseDir, "objects", name)

	if _, err := os.Stat(path); err == nil {
		return nil
	}

	tmp := filepath.Join(s.baseDir, "tmp", name+".tmp")

	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	defer os.Remove(tmp)

	got := sha256.Sum256(data)
	if got != hash {
		return fmt.Errorf("hash mismatch: expected %x, got %x", hash, got)
	}

	return os.Rename(tmp, path)
}

func (s *CASBlobStore) GetFile(hash [32]byte) (*os.File, error) {
	name := hex.EncodeToString(hash[:])
	path := filepath.Join(s.baseDir, "objects", name)

	return os.Open(path)
}

func (s *CASBlobStore) Get(hash [32]byte) ([]byte, error) {
	f, err := s.GetFile(hash)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	
	return io.ReadAll(f)
}