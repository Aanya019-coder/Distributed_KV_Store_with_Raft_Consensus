package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"raft-kv/gateway"
)

func main() {
	nodesStr := flag.String("nodes", "", "Comma-separated list of Raft node HTTP addresses (e.g. 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003)")
	httpAddr := flag.String("http-addr", ":7000", "Gateway HTTP listen address")
	gatewayToken := flag.String("gateway-token", "", "Bearer token clients present to the gateway (falls back to GATEWAY_TOKEN env)")
	clusterToken := flag.String("cluster-token", "", "Internal cluster Bearer token forwarded to Raft nodes (falls back to KV_API_TOKEN env)")
	frontendDir := flag.String("frontend-dir", "frontend/dist", "Path to the built frontend static files")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "How often to poll Raft nodes for status")
	flag.Parse()

	// Resolve tokens from env if not set via flags
	gToken := *gatewayToken
	if gToken == "" {
		gToken = os.Getenv("GATEWAY_TOKEN")
	}
	cToken := *clusterToken
	if cToken == "" {
		cToken = os.Getenv("KV_API_TOKEN")
	}

	// Resolve nodes from env if not set via flag
	nodes := *nodesStr
	if nodes == "" {
		nodes = os.Getenv("NODE_ADDRS")
	}
	if nodes == "" {
		nodes = "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
	}
	nodeAddrs := strings.Split(nodes, ",")
	for i, addr := range nodeAddrs {
		nodeAddrs[i] = strings.TrimSpace(addr)
	}

	log.Printf("[GATEWAY] Starting with nodes: %v", nodeAddrs)
	log.Printf("[GATEWAY] Auth enabled: %v", gToken != "")
	log.Printf("[GATEWAY] Frontend dir: %s", *frontendDir)

	cfg := gateway.Config{
		NodeAddrs:    nodeAddrs,
		GatewayToken: gToken,
		ClusterToken: cToken,
		FrontendDir:  *frontendDir,
		PollInterval: *pollInterval,
	}

	gw := gateway.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gw.Start(ctx)

	srv := &http.Server{
		Addr:         *httpAddr,
		Handler:      gw.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // SSE connections are long-lived
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("[GATEWAY] Listening on http://%s", *httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[GATEWAY] Server error: %v", err)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[GATEWAY] Shutting down...")
	cancel()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
	log.Println("[GATEWAY] Shutdown complete.")
}
