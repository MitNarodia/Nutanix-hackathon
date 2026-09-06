package assembler

import (
	"fmt"
	"io"
	"os"

	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

func Assemble(cas *store.CASBlobStore, chunkHashes [][32]byte, destPath string) error {
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer out.Close()

	for i, hash := range chunkHashes {
		chunkFile, err := cas.GetFile(hash)
		if err != nil {
			return fmt.Errorf("missing chunk %d (%x): %w", i, hash[:4], err)
		}

		_, err = io.Copy(out, chunkFile)
		chunkFile.Close()

		if err != nil {
			return fmt.Errorf("failed to write chunk %d to file: %w", i, err)
		}
	}

	return nil
}