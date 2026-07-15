package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	raftproto "raft-kv/proto"
	"raft-kv/raft"
	"raft-kv/api"
	"raft-kv/transport"
)

func main() {
	nodeID := flag.String("id", "", "Unique identifier for this node")
	grpcAddr := flag.String("grpc-addr", "", "gRPC address to bind to (e.g. localhost:9001)")
	httpAddr := flag.String("http-addr", "", "HTTP API address to bind to (e.g. localhost:8001)")
	peersStr := flag.String("peers", "", "Comma-separated list of peer gRPC addresses (e.g. node2=localhost:9002,node3=localhost:9003)")
	peerHTTPStr := flag.String("peer-http", "", "Comma-separated list of peer HTTP addresses (e.g. node2=localhost:8002,node3=localhost:8003)")
	storageDir := flag.String("storage-dir", "", "Directory for WAL and snapshots")
	caCertPath := flag.String("ca-cert", "certs/ca.pem", "Path to CA certificate file")
	nodeCertPath := flag.String("node-cert", "certs/node.pem", "Path to Node certificate file")
	nodeKeyPath := flag.String("node-key", "certs/node.key", "Path to Node private key file")
	peerServerName := flag.String("peer-server-name", "localhost", "Expected Server Name in peer certificates")
	apiToken := flag.String("api-token", "", "Bearer token required for API writes (falls back to KV_API_TOKEN env var)")
	allowUnauthReads := flag.Bool("allow-unauth-reads", false, "Allow unauthenticated GET reads")
	snapshotThreshold := flag.Int("snapshot-threshold", 1000, "Compacted log threshold for snapshotting")
	debug := flag.Bool("debug", false, "Enable debug verbose logging")
	flag.Parse()

	if *nodeID == "" || *grpcAddr == "" || *httpAddr == "" || *storageDir == "" {
		log.Fatalf("[ERROR] Missing required flags. Run with --help to see all options.")
	}

	// Resolve API Token from flag or env
	token := *apiToken
	if token == "" {
		token = os.Getenv("KV_API_TOKEN")
	}

	// Parse peers
	peers := make(map[string]string)
	if *peersStr != "" {
		for _, pair := range strings.Split(*peersStr, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 {
				peers[parts[0]] = parts[1]
			}
		}
	}

	// Parse peer HTTP addresses
	peerHTTPAddrs := make(map[string]string)
	if *peerHTTPStr != "" {
		for _, pair := range strings.Split(*peerHTTPStr, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 {
				peerHTTPAddrs[parts[0]] = parts[1]
			}
		}
	}

	log.Printf("[INFO] Bootstrapping Raft node %s...", *nodeID)

	// Create and initialize Raft node
	r, err := raft.NewRaft(
		*nodeID,
		peers,
		*storageDir,
		*caCertPath,
		*nodeCertPath,
		*nodeKeyPath,
		*peerServerName,
		*debug,
		*snapshotThreshold,
	)
	if err != nil {
		log.Fatalf("[ERROR] Failed to initialize Raft: %v", err)
	}

	// Start gRPC Server for peer communication using mTLS
	creds, err := transport.LoadServerCredentials(*caCertPath, *nodeCertPath, *nodeKeyPath)
	if err != nil {
		log.Fatalf("[ERROR] Failed to load server TLS credentials: %v", err)
	}

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("[ERROR] Failed to listen on gRPC address %s: %v", *grpcAddr, err)
	}

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	raftproto.RegisterRaftServiceServer(grpcServer, r)

	go func() {
		log.Printf("[INFO] gRPC server listening on %s (mTLS enabled)", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("[ERROR] gRPC server failure: %v", err)
		}
	}()

	// Start Raft node internal loops
	if err := r.Start(); err != nil {
		log.Fatalf("[ERROR] Failed to start Raft node: %v", err)
	}

	// Start HTTP REST API
	apiServer := api.NewServer(r, token, *allowUnauthReads, peerHTTPAddrs)
	if err := apiServer.Start(*httpAddr); err != nil {
		log.Fatalf("[ERROR] Failed to start HTTP API: %v", err)
	}
	log.Printf("[INFO] HTTP REST API listening on http://%s (API Token Auth: %t)", *httpAddr, token != "")

	// Graceful shutdown handle
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[INFO] Shutting down node gracefully...")
	apiServer.Stop()
	grpcServer.GracefulStop()
	r.Stop()
	log.Println("[INFO] Node successfully stopped.")
}
