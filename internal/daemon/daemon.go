package daemon

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/MitNarodia/Nutanix-hackathon/internal/auth"
	"github.com/MitNarodia/Nutanix-hackathon/internal/chunker"
	"github.com/MitNarodia/Nutanix-hackathon/internal/control"
	"github.com/MitNarodia/Nutanix-hackathon/internal/dataplane"
	"github.com/MitNarodia/Nutanix-hackathon/internal/discovery"
	"github.com/MitNarodia/Nutanix-hackathon/internal/merkle"
	"github.com/MitNarodia/Nutanix-hackathon/internal/orchestrator"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
	"github.com/MitNarodia/Nutanix-hackathon/internal/watcher"
	pb "github.com/MitNarodia/Nutanix-hackathon/proto"
)

// antiEntropyFanout keeps each reconciliation round cheap (only a few
// catalog fetches per node).
const (
	antiEntropyInterval = 20 * time.Second
	antiEntropyFanout   = 3
	quietWriteTTL       = 5 * time.Second
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
	Engine     *orchestrator.Engine
	grpcServer *grpc.Server
	grpcPort   int
	httpPort   int
	secret     []byte

	clock   atomic.Uint64
	quiet   *quietSet
	syncing sync.Map // fileID -> struct{}
}

// NewDaemon wires up a node. syncDir is watched and kept in sync; dataDir
// holds internal state (CAS objects + the bbolt index).
func NewDaemon(name string, syncDir string, dataDir string, httpPort int, grpcPort int, gossipPort int, secret []byte) (*Daemon, error) {
	if isSubPath(syncDir, dataDir) {
		return nil, fmt.Errorf("data dir %q must not be inside sync dir %q (it would be watched and re-ingested as data)", dataDir, syncDir)
	}

	os.MkdirAll(syncDir, 0755)
	os.MkdirAll(dataDir, 0755)

	cas, err := store.NewCASBlobStore(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to init CAS: %w", err)
	}

	meta, err := store.NewBoltMetaStore(filepath.Join(dataDir, "meta.db"))
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
	gossip := discovery.NewGossipNode(name, gossipPort, httpPort, grpcPort)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryServerInterceptor(secret)),
	)

	controlService := control.NewControlServer(name, meta, cas)
	pb.RegisterControlServiceServer(grpcServer, controlService)

	d := &Daemon{
		Name:       name,
		BaseDir:    syncDir,
		CAS:        cas,
		MetaStore:  meta,
		Gossip:     gossip,
		Server:     server,
		Client:     client,
		grpcServer: grpcServer,
		grpcPort:   grpcPort,
		httpPort:   httpPort,
		secret:     secret,
		quiet:      newQuietSet(quietWriteTTL),
	}

	engine := orchestrator.NewEngine(name, secret, syncDir, cas, meta, client)
	engine.MarkQuiet = d.quiet.Mark
	engine.Announce = d.announce
	d.Engine = engine

	w, err := watcher.NewWatcher(syncDir, func(filePath string) {
		d.HandleFileChange(filePath)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init watcher: %w", err)
	}

	d.Watcher = w

	return d, nil
}

// nextTS bumps this node's Lamport clock for a new local write.
func (d *Daemon) nextTS() uint64 {
	return d.clock.Add(1)
}

// observeTS merges a timestamp seen from a peer into our clock
// (clock = max(local, remote)) so later local writes sort after it.
func (d *Daemon) observeTS(remote uint64) {
	for {
		cur := d.clock.Load()
		if remote <= cur {
			return
		}
		if d.clock.CompareAndSwap(cur, remote) {
			return
		}
	}
}

// announce broadcasts a file's current state over gossip.
func (d *Daemon) announce(fileID string, root [32]byte, lamportTS uint64) {
	d.Gossip.Broadcast(discovery.FileAnnouncement{
		FileID:     fileID,
		MerkleRoot: root,
		LamportTS:  lamportTS,
		SourceNode: d.Name,
	})
}

func (d *Daemon) HandleFileChange(path string) {
	if filepath.Ext(path) == ".db" {
		return
	}

	if d.quiet.IsQuiet(path) {
		return
	}

	// normalize so peers can request the same fileID regardless of OS
	relPath, err := filepath.Rel(d.BaseDir, path)
	if err != nil {
		relPath = filepath.Base(path)
	}
	fileID := filepath.ToSlash(relPath)

	c := chunker.NewFastCDCChunker(8192)
	chunks, err := c.ChunkFile(path)
	if err != nil {
		log.Printf("[%s] chunking failed for %s: %v", d.Name, path, err)
		return
	}

	root := merkle.ComputeRoot(chunks)

	if existing, err := d.MetaStore.GetFileMeta(fileID); err == nil && existing.MerkleRoot == root {
		// content didn't actually change (e.g. a touch) - skip
		return
	}

	var hashes [][32]byte
	var size uint64

	for _, ch := range chunks {
		d.CAS.Put(ch.Hash, ch.Data)
		hashes = append(hashes, ch.Hash)
		size += uint64(len(ch.Data))
	}

	ts := d.nextTS()

	meta := store.FileMeta{
		FileID:      fileID,
		MerkleRoot:  root,
		LamportTS:   ts,
		ChunkHashes: hashes,
		SizeBytes:   size,
	}

	d.MetaStore.PutFileMeta(meta)

	fmt.Printf(
		"\n[%s] File change detected: %s (ID: %s)\n"+
			"    -> indexed %d chunks, merkle root %x, ts=%d\n",
		d.Name, path, fileID, len(chunks), root[:4], ts,
	)

	d.announce(fileID, root, ts)
}

// handleAnnouncement reacts to a peer's gossiped file-change notification -
// we only kick off a sync once we hear something actually changed, instead
// of polling every peer's catalog on a timer.
func (d *Daemon) handleAnnouncement(ann discovery.FileAnnouncement) {
	if ann.SourceNode == d.Name {
		return
	}

	d.observeTS(ann.LamportTS)

	if local, err := d.MetaStore.GetFileMeta(ann.FileID); err == nil {
		if local.MerkleRoot == ann.MerkleRoot {
			return // already in sync
		}
		if !d.remoteWins(local.LamportTS, d.Name, ann.LamportTS, ann.SourceNode) {
			return // our local version wins the conflict; keep it
		}
	}

	d.triggerSync(ann.FileID)
}

// remoteWins is last-writer-wins conflict resolution: higher Lamport
// timestamp wins, ties broken by node name so every node picks the same
// winner independently.
func (d *Daemon) remoteWins(localTS uint64, localNode string, remoteTS uint64, remoteNode string) bool {
	if remoteTS != localTS {
		return remoteTS > localTS
	}
	return remoteNode > localNode
}

// triggerSync starts a Pull for fileID if one isn't already running,
// against the full set of known peers so rarest-first scheduling has
// something to work with.
func (d *Daemon) triggerSync(fileID string) {
	if _, alreadySyncing := d.syncing.LoadOrStore(fileID, struct{}{}); alreadySyncing {
		return
	}

	peers := d.Gossip.GetActivePeers()

	go func() {
		defer d.syncing.Delete(fileID)

		log.Printf("[%s] Syncing %s from %d known peer(s)...", d.Name, fileID, len(peers))

		if err := d.Engine.Pull(context.Background(), fileID, peers); err != nil {
			log.Printf("[%s] Sync failed for %s: %v", d.Name, fileID, err)
		} else {
			log.Printf("[%s] Successfully synced %s!", d.Name, fileID)
		}
	}()
}

// antiEntropyLoop is the safety net under gossip: every interval, compare
// catalogs with a small random sample of peers to catch announcements lost
// to UDP drops and to bring newly-joined nodes up to date.
func (d *Daemon) antiEntropyLoop(ctx context.Context) {
	ticker := time.NewTicker(antiEntropyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers := d.Gossip.GetActivePeers()
			if len(peers) == 0 {
				continue
			}

			rand.Shuffle(len(peers), func(i, j int) {
				peers[i], peers[j] = peers[j], peers[i]
			})

			if len(peers) > antiEntropyFanout {
				peers = peers[:antiEntropyFanout]
			}

			for _, p := range peers {
				go d.reconcileWithPeer(ctx, p)
			}
		}
	}
}

func (d *Daemon) reconcileWithPeer(ctx context.Context, peer discovery.PeerInfo) {
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	remoteCatalog, err := d.Client.FetchCatalog(ctxTimeout, peer.HTTPAddr())
	if err != nil {
		return
	}

	for _, entry := range remoteCatalog {
		if filepath.Ext(entry.FileID) == ".db" {
			continue
		}

		local, err := d.MetaStore.GetFileMeta(entry.FileID)
		if err == nil {
			if local.MerkleRoot == entry.MerkleRoot {
				continue
			}
			if !d.remoteWins(local.LamportTS, d.Name, entry.LamportTS, peer.Name) {
				continue
			}
		}

		d.observeTS(entry.LamportTS)
		d.triggerSync(entry.FileID)
	}
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

	d.Gossip.OnAnnouncement(d.handleAnnouncement)

	if err := d.Gossip.Start(seedPeers); err != nil {
		log.Fatalf("[%s] Control plane failed to start: %v", d.Name, err)
	}

	if err := d.Watcher.Start(ctx); err != nil {
		log.Printf("[%s] Warning: Watcher failed to start: %v", d.Name, err)
	}

	go d.antiEntropyLoop(ctx)

	fmt.Printf("[%s] Daemon online and watching %s.\n", d.Name, d.BaseDir)
}

func (d *Daemon) Stop() {
	d.Gossip.Stop()
	d.grpcServer.GracefulStop()
	d.MetaStore.Close()
}
