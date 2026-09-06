package control

import (
	"bytes"
	"context"
	"time"

	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
	pb "github.com/MitNarodia/Nutanix-hackathon/proto"
)

type ControlServer struct {
	pb.UnimplementedControlServiceServer
	nodeID    string
	metaStore *store.BoltMetaStore
	casStore  *store.CASBlobStore
	startTime time.Time
}

func NewControlServer(nodeID string, meta *store.BoltMetaStore, cas *store.CASBlobStore) *ControlServer {
	return &ControlServer{
		nodeID:    nodeID,
		metaStore: meta,
		casStore:  cas,
		startTime: time.Now(),
	}
}

func (s *ControlServer) DiffMerkleTree(ctx context.Context, req *pb.DiffRequest) (*pb.DiffResponse, error) {
	meta, err := s.metaStore.GetFileMeta(req.FileId)
	if err != nil {
		return &pb.DiffResponse{
			FileId:   req.FileId,
			IsSynced: false,
		}, nil
	}

	if bytes.Equal(meta.MerkleRoot[:], req.LocalMerkleRoot) {
		return &pb.DiffResponse{
			FileId:           req.FileId,
			RemoteMerkleRoot: meta.MerkleRoot[:],
			IsSynced:         true,
		}, nil
	}

	knownSet := make(map[[32]byte]bool)

	for _, rawHash := range req.KnownChunkHashes {
		if len(rawHash) == 32 {
			var h [32]byte
			copy(h[:], rawHash)
			knownSet[h] = true
		}
	}

	var missing [][]byte

	for _, chunkHash := range meta.ChunkHashes {
		if !knownSet[chunkHash] {
			hashCopy := make([]byte, 32)
			copy(hashCopy, chunkHash[:])
			missing = append(missing, hashCopy)
		}
	}

	return &pb.DiffResponse{
		FileId:              req.FileId,
		RemoteMerkleRoot:    meta.MerkleRoot[:],
		IsSynced:            false,
		MissingChunkHashes: missing,
	}, nil
}

func (s *ControlServer) GetChunkAvailability(ctx context.Context, req *pb.AvailabilityRequest) (*pb.AvailabilityResponse, error) {
	var available [][]byte

	for _, rawHash := range req.ChunkHashes {
		if len(rawHash) != 32 {
			continue
		}

		var h [32]byte
		copy(h[:], rawHash)

		if f, err := s.casStore.GetFile(h); err == nil {
			f.Close()
			available = append(available, rawHash)
		}
	}

	return &pb.AvailabilityResponse{
		AvailableChunkHashes: available,
	}, nil
}

func (s *ControlServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		NodeId:     s.nodeID,
		UptimeSecs: uint64(time.Since(s.startTime).Seconds()),
	}, nil
}