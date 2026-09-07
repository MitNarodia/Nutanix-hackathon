package store

import (
	"encoding/json"
	"fmt"

	"go.etcd.io/bbolt"
)

type FileMeta struct {
	FileID      string
	MerkleRoot  [32]byte
	LamportTS   uint64
	ChunkHashes [][32]byte
	SizeBytes   uint64
}

type BoltMetaStore struct {
	db *bbolt.DB
}

func NewBoltMetaStore(dbPath string) (*BoltMetaStore, error) {
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("files"))
		return err
	})

	return &BoltMetaStore{db: db}, err
}

func (s *BoltMetaStore) PutFileMeta(meta FileMeta) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		bucket := tx.Bucket([]byte("files"))
		return bucket.Put([]byte(meta.FileID), data)
	})
}

func (s *BoltMetaStore) GetFileMeta(fileID string) (*FileMeta, error) {
	var meta FileMeta

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("files"))
		data := bucket.Get([]byte(fileID))

		if data == nil {
			return fmt.Errorf("file not found")
		}

		return json.Unmarshal(data, &meta)
	})

	return &meta, err
}

// CatalogEntry is the lightweight per-file summary served to peers so they
// can decide whether to sync without fetching full chunk lists for every
// file in the store.
type CatalogEntry struct {
	FileID     string
	MerkleRoot [32]byte
	LamportTS  uint64
}

func (s *BoltMetaStore) ListCatalog() ([]CatalogEntry, error) {
	var entries []CatalogEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("files"))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var m FileMeta
			if err := json.Unmarshal(v, &m); err != nil {
				return nil
			}
			entries = append(entries, CatalogEntry{
				FileID:     m.FileID,
				MerkleRoot: m.MerkleRoot,
				LamportTS:  m.LamportTS,
			})
			return nil
		})
	})
	return entries, err
}

func (s *BoltMetaStore) Close() error {
	return s.db.Close()
}
