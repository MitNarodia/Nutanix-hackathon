package discovery

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
)

// FileAnnouncement is gossiped cluster-wide whenever a node's local copy of a
// file changes, so peers can react without polling each other's catalogs.
type FileAnnouncement struct {
	FileID     string
	MerkleRoot [32]byte
	LamportTS  uint64
	SourceNode string
}

// PeerInfo describes a live cluster member and the ports it advertises for
// the data plane (HTTP) and control plane (gRPC), read from gossip NodeMeta.
type PeerInfo struct {
	Name     string
	Addr     string
	HTTPPort int
	GRPCPort int
}

func (p PeerInfo) HTTPAddr() string { return fmt.Sprintf("%s:%d", p.Addr, p.HTTPPort) }
func (p PeerInfo) GRPCAddr() string { return fmt.Sprintf("%s:%d", p.Addr, p.GRPCPort) }

type GossipNode struct {
	list       *memberlist.Memberlist
	port       int
	name       string
	httpPort   int
	grpcPort   int
	queue      *memberlist.TransmitLimitedQueue
	onAnnounce func(FileAnnouncement)
}

func NewGossipNode(name string, bindPort int, httpPort int, grpcPort int) *GossipNode {
	return &GossipNode{
		port:     bindPort,
		name:     name,
		httpPort: httpPort,
		grpcPort: grpcPort,
	}
}

// OnAnnouncement registers the callback invoked whenever a peer's file
// change announcement arrives via gossip. Must be called before Start.
func (g *GossipNode) OnAnnouncement(fn func(FileAnnouncement)) {
	g.onAnnounce = fn
}

func (g *GossipNode) Start(seedPeers []string) error {
	g.queue = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			if g.list == nil {
				return 1
			}
			return g.list.NumMembers()
		},
		RetransmitMult: 3,
	}

	config := memberlist.DefaultLANConfig()
	config.BindPort = g.port
	config.Name = g.name
	config.Logger = log.New(log.Writer(), "", 0)
	config.Delegate = &portDelegate{
		httpPort: g.httpPort,
		grpcPort: g.grpcPort,
		queue:    g.queue,
		onMsg:    g.handleMsg,
	}

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

func (g *GossipNode) handleMsg(msg []byte) {
	if g.onAnnounce == nil {
		return
	}

	var ann FileAnnouncement
	if err := json.Unmarshal(msg, &ann); err != nil {
		return
	}

	g.onAnnounce(ann)
}

// Broadcast pushes a file announcement out to the cluster via memberlist's
// gossip queue, piggybacked on SWIM probes.
func (g *GossipNode) Broadcast(ann FileAnnouncement) {
	if g.queue == nil {
		return
	}

	data, err := json.Marshal(ann)
	if err != nil {
		return
	}

	g.queue.QueueBroadcast(&fileBroadcast{msg: data})
}

// GetActivePeers returns every other known live member along with the
// HTTP/gRPC ports it advertised over gossip NodeMeta.
func (g *GossipNode) GetActivePeers() []PeerInfo {
	var peers []PeerInfo

	for _, member := range g.list.Members() {
		if member.Name == g.name {
			continue
		}

		if len(member.Meta) != 4 {
			continue
		}

		peers = append(peers, PeerInfo{
			Name:     member.Name,
			Addr:     member.Addr.String(),
			HTTPPort: int(binary.BigEndian.Uint16(member.Meta[0:2])),
			GRPCPort: int(binary.BigEndian.Uint16(member.Meta[2:4])),
		})
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

type fileBroadcast struct {
	msg []byte
}

func (b *fileBroadcast) Invalidates(other memberlist.Broadcast) bool { return false }
func (b *fileBroadcast) Message() []byte                             { return b.msg }
func (b *fileBroadcast) Finished()                                   {}

type portDelegate struct {
	httpPort int
	grpcPort int
	queue    *memberlist.TransmitLimitedQueue
	onMsg    func([]byte)
}

func (d *portDelegate) NodeMeta(limit int) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(d.httpPort))
	binary.BigEndian.PutUint16(buf[2:4], uint16(d.grpcPort))
	return buf
}

func (d *portDelegate) NotifyMsg(msg []byte) {
	if len(msg) == 0 || d.onMsg == nil {
		return
	}

	cp := append([]byte(nil), msg...)
	d.onMsg(cp)
}

func (d *portDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	if d.queue == nil {
		return nil
	}
	return d.queue.GetBroadcasts(overhead, limit)
}

func (d *portDelegate) LocalState(join bool) []byte            { return nil }
func (d *portDelegate) MergeRemoteState(buf []byte, join bool) {}
