package assembler

import (
	"errors"
	"os"
	"sync"
)

type SparseFile struct {
	mu         sync.Mutex
	file       *os.File
	bitmap     []bool
	chunksDone int
	total      int
}

func NewSparseFile(path string, totalSize int64, totalChunks int) (*SparseFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	if err := f.Truncate(totalSize); err != nil {
		f.Close()
		return nil, err
	}

	return &SparseFile{
		file:   f,
		bitmap: make([]bool, totalChunks),
		total:  totalChunks,
	}, nil
}

func (s *SparseFile) WriteChunk(chunkIndex int, offset int64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if chunkIndex < 0 || chunkIndex >= s.total {
		return errors.New("chunk index out of bounds")
	}

	if s.bitmap[chunkIndex] {
		return nil
	}

	_, err := s.file.WriteAt(data, offset)
	if err != nil {
		return err
	}

	s.bitmap[chunkIndex] = true
	s.chunksDone++

	return nil
}

func (s *SparseFile) IsComplete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.chunksDone == s.total
}

func (s *SparseFile) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.file.Close()
}