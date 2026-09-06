package assembler

import (
	"fmt"
	"io"
	"os"

	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

// Assemble reconstructs the original file using the sequence of chunk hashes.
func Assemble(cas *store.CASBlobStore, chunkHashes [][32]byte, destPath string) error {
	// Create (or overwrite) the final destination file
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer out.Close() // Automatically close when we are done

	for i, hash := range chunkHashes {
		// 1. Open the raw chunk from the local CAS vault
		chunkFile, err := cas.GetFile(hash)
		if err != nil {
			return fmt.Errorf("missing chunk %d (%x): %w", i, hash[:4], err)
		}
		
		// 2. Stream the bytes directly from the chunk into the final file.
		// io.Copy automatically handles buffering so we don't blow up RAM.
		_, err = io.Copy(out, chunkFile)
		
		chunkFile.Close() // Close immediately to free OS file descriptors
		if err != nil {
			return fmt.Errorf("failed to write chunk %d to file: %w", i, err)
		}
	}

	return nil
}