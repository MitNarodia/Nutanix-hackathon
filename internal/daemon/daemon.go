package daemon

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/MitNarodia/Nutanix-hackathon/internal/auth"
	"github.com/MitNarodia/Nutanix-hackathon/internal/chunker"
	"github.com/MitNarodia/Nutanix-hackathon/internal/control"
	"github.com/MitNarodia/Nutanix-hackathon/internal/dataplane"
	"github.com/MitNarodia/Nutanix-hackathon/internal/discovery"
	"github.com/MitNarodia/Nutanix-hackathon/internal/merkle"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
	"github.com/MitNarodia/Nutanix-hackathon/internal/watcher"
	pb "github.com/MitNarodia/Nutanix-hackathon/proto"
)

type Daemon struct {
	Name       string
	BaseDir    string
	CAS        *store.CASBlobStore
	MetaStore  *store.BoltMetaStore
	Gossip     *discovery.GossipNode
	Server     *dataplane.Server
	Client     *dataplane.Client
	Watcher    *watcher.Watcher
	grpcServer *grpc.Server
	grpcPort   int
}

func NewDaemon(name string, baseDir string, httpPort int, grpcPort int, gossipPort int, secret []byte) (*Daemon, error) {
	os.MkdirAll(baseDir, 0755)

	cas, err := store.NewCASBlobStore(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init CAS: %w", err)
	}

	meta, err := store.NewBoltMetaStore(filepath.Join(baseDir, "meta.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to init bbolt: %w", err)
	}

	authLayer := auth.NewAuth(secret, name)

	server := dataplane.NewServer(
		fmt.Sprintf("0.0.0.0:%d", httpPort),
		authLayer,
		cas,
		meta,
		50,
	)

	client := dataplane.NewClient(authLayer, cas)
	gossip := discovery.NewGossipNode(name, gossipPort)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryServerInterceptor(secret)),
	)

	controlService := control.NewControlServer(name, meta, cas)
	pb.RegisterControlServiceServer(grpcServer, controlService)

	d := &Daemon{
		Name:       name,
		BaseDir:    baseDir,
		CAS:        cas,
		MetaStore:  meta,
		Gossip:     gossip,
		Server:     server,
		Client:     client,
		grpcServer: grpcServer,
		grpcPort:   grpcPort,
	}

	w, err := watcher.NewWatcher(baseDir, func(filePath string) {
		d.HandleFileChange(filePath)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init watcher: %w", err)
	}

	d.Watcher = w

	return d, nil
}

func (d *Daemon) HandleFileChange(path string) {
	if filepath.Ext(path) == ".db" || filepath.Base(path) == "restored_file.txt" {
		return
	}

	fmt.Printf("\n[%s] File change detected: %s\n", d.Name, path)

	c := chunker.NewFastCDCChunker(8192)
	chunks, err := c.ChunkFile(path)
	if err != nil {
		log.Printf("Error !! .... Auto-chunk failed for %s: %v", path, err)
		return
	}

	var hashes [][32]byte

	for _, ch := range chunks {
		d.CAS.Put(ch.Hash, ch.Data)
		hashes = append(hashes, ch.Hash)
	}

	meta := store.FileMeta{
		FileID:      path,
		MerkleRoot:  merkle.ComputeRoot(chunks),
		ChunkHashes: hashes,
	}

	d.MetaStore.PutFileMeta(meta)

	fmt.Printf(
		"   -> Auto-indexed %d chunks. Merkle Root: %x\n",
		len(chunks),
		meta.MerkleRoot[:4],
	)
}

func (d *Daemon) Start(ctx context.Context, seedPeers []string) {
	go func() {
		if err := d.Server.Start(ctx); err != nil {
			log.Printf("[%s] Data plane server exited: %v", d.Name, err)
		}
	}()

	go func() {
		lis, err := net.Listen(
			"tcp",
			fmt.Sprintf("0.0.0.0:%d", d.grpcPort),
		)
		if err != nil {
			log.Fatalf("[%s] Failed to listen on gRPC port: %v", d.Name, err)
		}

		fmt.Printf(
			"gRPC Control plane listening on 0.0.0.0:%d\n",
			d.grpcPort,
		)

		if err := d.grpcServer.Serve(lis); err != nil {
			log.Printf("[%s] gRPC server exited: %v", d.Name, err)
		}
	}()

	if err := d.Gossip.Start(seedPeers); err != nil {
		log.Fatalf("[%s] Control plane failed to start: %v", d.Name, err)
	}

	if err := d.Watcher.Start(ctx); err != nil {
		log.Printf("[%s] Warning: Watcher failed to start: %v", d.Name, err)
	}

	fmt.Printf("[%s] Daemon online and watching %s.\n", d.Name, d.BaseDir)
}

func (d *Daemon) Stop() {
	d.Gossip.Stop()
	d.grpcServer.GracefulStop()
	d.MetaStore.Close()
}