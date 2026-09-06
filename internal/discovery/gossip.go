package discovery

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
)

type GossipNode struct {
	list *memberlist.Memberlist
	port int
	name string
}

func NewGossipNode(name string, bindPort int) *GossipNode {
	return &GossipNode{
		port: bindPort,
		name: name,
	}
}

func (g *GossipNode) Start(seedPeers []string) error {
	config := memberlist.DefaultLANConfig()
	config.BindPort = g.port
	config.Name = g.name
	config.Logger = log.New(log.Writer(), "", 0)

	list, err := memberlist.Create(config)
	if err != nil {
		return fmt.Errorf("failed to create memberlist: %w", err)
	}

	g.list = list

	if len(seedPeers) > 0 {
		fmt.Printf("[%s] Attempting to join cluster via %v...\n", g.name, seedPeers)

		_, err := g.list.Join(seedPeers)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
	}

	return nil
}

func (g *GossipNode) GetActivePeers() []string {
	var peers []string

	for _, member := range g.list.Members() {
		if member.Name != g.name {
			peers = append(peers, member.Name)
		}
	}

	return peers
}

func (g *GossipNode) Stop() {
	if g.list != nil {
		g.list.Leave(2 * time.Second)
		g.list.Shutdown()
		g.list = nil
	}
}