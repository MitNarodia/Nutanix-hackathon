package chunker

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/jotfs/fastcdc-go"
)

type Chunk struct {
	Hash     [32]byte
	Offset   uint64
	Size     uint32
	Sequence uint32
	Data     []byte
}

type FastCDCChunker struct {
	opts fastcdc.Options
}

func NewFastCDCChunker(avgSize int) *FastCDCChunker {
	return &FastCDCChunker{
		opts: fastcdc.Options{
			AverageSize: avgSize,
			MinSize:     avgSize / 4,
			MaxSize:     avgSize * 8,
		},
	}
}

func (c *FastCDCChunker) ChunkFile(path string) ([]Chunk, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open file %s: %w", path, err)
	}
	defer file.Close()

	cdc, err := fastcdc.NewChunker(file, c.opts)
	if err != nil {
		return nil, fmt.Errorf("could not create chunker: %w", err)
	}

	var chunks []Chunk
	var seq uint32

	for {
		part, err := cdc.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("chunking failed at %d: %w", seq, err)
		}

		hash := sha256.Sum256(part.Data)

		chunks = append(chunks, Chunk{
			Hash:     hash,
			Offset:   uint64(part.Offset),
			Size:     uint32(part.Length),
			Sequence: seq,
			Data:     part.Data,
		})

		seq++
	}

	return chunks, nil
}