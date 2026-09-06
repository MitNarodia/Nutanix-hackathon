package control

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MitNarodia/Nutanix-hackathon/internal/auth"
	pb "github.com/MitNarodia/Nutanix-hackathon/proto"
)

type ControlClient struct {
	conn   *grpc.ClientConn
	client pb.ControlServiceClient
}

func NewControlClient(targetAddr string, secret []byte, nodeID string) (*ControlClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		targetAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(
			auth.UnaryClientInterceptor(secret, nodeID),
		),
	)
	if err != nil {
		return nil, err
	}

	return &ControlClient{
		conn:   conn,
		client: pb.NewControlServiceClient(conn),
	}, nil
}

func (c *ControlClient) Close() {
	c.conn.Close()
}

func (c *ControlClient) Diff(
	ctx context.Context,
	fileID string,
	localRoot []byte,
	knownChunks [][]byte,
) (*pb.DiffResponse, error) {
	req := &pb.DiffRequest{
		FileId:           fileID,
		LocalMerkleRoot:  localRoot,
		KnownChunkHashes: knownChunks,
	}

	return c.client.DiffMerkleTree(ctx, req)
}

func (c *ControlClient) CheckAvailability(
	ctx context.Context,
	chunks [][]byte,
) ([][]byte, error) {
	req := &pb.AvailabilityRequest{
		ChunkHashes: chunks,
	}

	resp, err := c.client.GetChunkAvailability(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.AvailableChunkHashes, nil
}