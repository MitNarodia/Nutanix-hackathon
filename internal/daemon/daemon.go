package daemon

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/MitNarodia/Nutanix-hackathon/internal/auth"
	"github.com/MitNarodia/Nutanix-hackathon/internal/dataplane"
	"github.com/MitNarodia/Nutanix-hackathon/internal/discovery"
	"github.com/MitNarodia/Nutanix-hackathon/internal/store"
)

type Daemon struct {
	Name      string
	CAS       *store.CASBlobStore
	MetaStore *store.BoltMetaStore
	Gossip    *discovery.GossipNode
	Server    *dataplane.Server
	Client    *dataplane.Client
}

func NewDaemon(name string, baseDir string, httpPort int, gossipPort int, secret []byte) (*Daemon, error) {
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
	
	server := dataplane.NewServer(fmt.Sprintf("0.0.0.0:%d", httpPort), authLayer, cas, meta, 50)
	client := dataplane.NewClient(authLayer, cas)
	gossip := discovery.NewGossipNode(name, gossipPort)

	return &Daemon{
		Name:      name,
		CAS:       cas,
		MetaStore: meta,
		Gossip:    gossip,
		Server:    server,
		Client:    client,
	}, nil
}

func (d *Daemon) Start(ctx context.Context, seedPeers []string) {

	go func() {
		if err := d.Server.Start(ctx); err != nil {
			log.Printf("[%s] Data plane server exited: %v", d.Name, err)
		}
	}()

	if err := d.Gossip.Start(seedPeers); err != nil {
		log.Fatalf("[%s] Control plane failed to start: %v", d.Name, err)
	}
	
	fmt.Printf("[%s] Daemon online and ready.\n", d.Name)
}

func (d *Daemon) Stop() {
	d.Gossip.Stop()
	d.MetaStore.Close()
}