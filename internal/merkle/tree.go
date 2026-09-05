package merkle

import (
	"crypto/sha256"

	"github.com/MitNarodia/Nutanix-hackathon/internal/chunker"
)

func ComputeRoot(chunks []chunker.Chunk) [32]byte {
	if len(chunks) == 0 {
		return [32]byte{}
	}

	var hashes [][32]byte

	for _, chunk := range chunks {
		hashes = append(hashes, chunk.Hash)
	}

	for len(hashes) > 1 {
		var newHashes [][32]byte

		for i := 0; i < len(hashes); i += 2 {
			if i+1 >= len(hashes) {
				newHashes = append(newHashes, hashes[i])
				continue
			}

			data := append(hashes[i][:], hashes[i+1][:]...)
			newHashes = append(newHashes, sha256.Sum256(data))
		}

		hashes = newHashes
	}

	return hashes[0]
}