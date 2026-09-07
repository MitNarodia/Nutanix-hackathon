// Command nusyncd runs a single NuSync node: it watches a directory,
// indexes changed files into content-addressed chunks, and gossips with
// peers to keep the directory in sync across the cluster.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/MitNarodia/Nutanix-hackathon/internal/daemon"
)

func main() {
	var (
		name       = flag.String("name", "", "unique node name (required)")
		dir        = flag.String("dir", "", "directory to watch and sync (required)")
		dataDir    = flag.String("data", "", "directory for internal state - CAS objects and metadata index (default: <dir>-nusync-data, a SIBLING of -dir, never inside it)")
		httpPort   = flag.Int("http", 9100, "data-plane HTTP port (chunk/meta/catalog transfer)")
		grpcPort   = flag.Int("grpc", 9000, "control-plane gRPC port (merkle diff / availability)")
		gossipPort = flag.Int("gossip", 7946, "SWIM gossip (memberlist) UDP/TCP port")
		apiPort    = flag.Int("api", 8080, "local CLI/API port (127.0.0.1 only)")
		seeds      = flag.String("seeds", "", "comma-separated gossip seed addresses (host:gossipPort) to join an existing cluster")
		secret     = flag.String("secret", "", "shared HMAC secret for authenticating with peers (required)")
	)
	flag.Parse()

	if *name == "" || *dir == "" || *secret == "" {
		flag.Usage()
		log.Fatal("missing required flags: -name, -dir, and -secret are all required")
	}

	if *dataDir == "" {
		// sibling of -dir, not a child - otherwise the watcher would pick
		// up the CAS's own blobs and re-ingest them as user files
		*dataDir = filepath.Clean(*dir) + "-nusync-data"
	}

	var seedPeers []string
	for _, s := range strings.Split(*seeds, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			seedPeers = append(seedPeers, s)
		}
	}

	d, err := daemon.NewDaemon(*name, *dir, *dataDir, *httpPort, *grpcPort, *gossipPort, []byte(*secret))
	if err != nil {
		log.Fatalf("failed to initialize daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("[%s] shutting down...", *name)
		cancel()
		d.Stop()
		os.Exit(0)
	}()

	d.Start(ctx, seedPeers)
	d.StartLocalAPI(*apiPort, []byte(*secret))
}
