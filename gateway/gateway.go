package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// NodeStatus mirrors the /status JSON from a Raft node.
type NodeStatus struct {
	NodeID               string   `json:"node_id"`
	Role                 string   `json:"role"`
	CurrentTerm          int64    `json:"current_term"`
	CommitIndex          int64    `json:"commit_index"`
	LastApplied          int64    `json:"last_applied"`
	LogLength            int64    `json:"log_length"`
	VotedFor             string   `json:"voted_for"`
	ClusterMembers       []string `json:"cluster_members"`
	ClusterSize          int      `json:"cluster_size"`
	JointConsensus       bool     `json:"joint_consensus"`
	PendingConfigChange  bool     `json:"pending_config_change"`
}

// ClusterOverview is the aggregated response from /cluster/overview.
type ClusterOverview struct {
	Nodes     []NodeStatus `json:"nodes"`
	LeaderID  string       `json:"leader_id"`
	LeaderURL string       `json:"leader_url"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// ClusterEvent is pushed over SSE to connected clients.
type ClusterEvent struct {
	Type      string          `json:"type"` // "state_update" | "leader_change" | "error"
	Payload   ClusterOverview `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}

// Config holds all gateway configuration.
type Config struct {
	NodeAddrs    []string // HTTP addresses of Raft nodes e.g. ["127.0.0.1:8001","127.0.0.1:8002","127.0.0.1:8003"]
	GatewayToken string   // token browser presents to gateway
	ClusterToken string   // token gateway presents to Raft nodes
	FrontendDir  string   // path to serve static frontend files
	PollInterval time.Duration
}

// Gateway is the main gateway service struct.
type Gateway struct {
	cfg           Config
	httpClient    *http.Client
	mu            sync.RWMutex
	nodeStatuses  map[string]NodeStatus // key = addr
	leaderAddr    string
	leaderID      string
	overview      ClusterOverview
	sseClients    map[chan ClusterEvent]struct{}
	sseMu         sync.Mutex
	reqIDCounter  atomic.Int64
}

// New creates a new Gateway.
func New(cfg Config) *Gateway {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	return &Gateway{
		cfg:          cfg,
		httpClient:   &http.Client{Timeout: 3 * time.Second},
		nodeStatuses: make(map[string]NodeStatus),
		sseClients:   make(map[chan ClusterEvent]struct{}),
	}
}

// Start begins the background leader-tracking loop.
func (g *Gateway) Start(ctx context.Context) {
	go g.pollLoop(ctx)
}

// Handler returns the http.Handler for the gateway.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()

	// Cluster endpoints (public)
	mux.HandleFunc("/cluster/overview", g.handleOverview)
	mux.HandleFunc("/cluster/events", g.handleSSE)
	mux.HandleFunc("/metrics/aggregate", g.handleAggregateMetrics)
	mux.HandleFunc("/healthz", g.handleHealthz)

	// KV proxy (auth required for writes)
	mux.HandleFunc("/kv/", g.handleKVProxy)

	// Admin proxy (auth required)
	mux.HandleFunc("/admin/", g.handleAdminProxy)

	// Serve frontend static files
	if g.cfg.FrontendDir != "" {
		fs := http.FileServer(http.Dir(g.cfg.FrontendDir))
		// SPA fallback: serve index.html for non-file routes
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// Try static file first
			path := g.cfg.FrontendDir + r.URL.Path
			if r.URL.Path == "/" || !fileExists(path) {
				http.ServeFile(w, r, g.cfg.FrontendDir+"/index.html")
				return
			}
			fs.ServeHTTP(w, r)
		})
	}

	return corsMiddleware(mux)
}

// ---- Polling loop ----

func (g *Gateway) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(g.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.pollAllNodes()
		}
	}
}

func (g *Gateway) pollAllNodes() {
	var wg sync.WaitGroup
	results := make([]NodeStatus, len(g.cfg.NodeAddrs))
	errors := make([]error, len(g.cfg.NodeAddrs))

	for i, addr := range g.cfg.NodeAddrs {
		wg.Add(1)
		go func(idx int, nodeAddr string) {
			defer wg.Done()
			status, err := g.fetchNodeStatus(nodeAddr)
			if err != nil {
				errors[idx] = err
				return
			}
			results[idx] = status
		}(i, addr)
	}
	wg.Wait()

	// Build new overview
	newOverview := ClusterOverview{
		UpdatedAt: time.Now(),
	}
	newLeaderAddr := ""
	newLeaderID := ""

	for i, addr := range g.cfg.NodeAddrs {
		if errors[i] != nil {
			// Include a placeholder for unreachable node
			results[i] = NodeStatus{NodeID: fmt.Sprintf("node%d(offline)", i+1)}
		}
		newOverview.Nodes = append(newOverview.Nodes, results[i])
		if results[i].Role == "leader" {
			newLeaderAddr = addr
			newLeaderID = results[i].NodeID
		}
	}
	newOverview.LeaderID = newLeaderID
	newOverview.LeaderURL = newLeaderAddr

	// Check if state changed — only push SSE event on change
	g.mu.Lock()
	changed := g.leaderID != newLeaderID ||
		(len(newOverview.Nodes) > 0 && len(g.overview.Nodes) > 0 &&
			newOverview.Nodes[0].CurrentTerm != g.overview.Nodes[0].CurrentTerm)
	g.leaderAddr = newLeaderAddr
	g.leaderID = newLeaderID
	g.overview = newOverview
	g.mu.Unlock()

	if changed || len(g.sseClients) > 0 {
		eventType := "state_update"
		if newLeaderID != "" {
			eventType = "state_update"
		}
		g.broadcastSSE(ClusterEvent{
			Type:      eventType,
			Payload:   newOverview,
			Timestamp: time.Now(),
		})
	}
}

func (g *Gateway) fetchNodeStatus(addr string) (NodeStatus, error) {
	resp, err := g.httpClient.Get("http://" + addr + "/status")
	if err != nil {
		return NodeStatus{}, err
	}
	defer resp.Body.Close()

	var status NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return NodeStatus{}, err
	}
	return status, nil
}

// ---- SSE ----

func (g *Gateway) broadcastSSE(event ClusterEvent) {
	g.sseMu.Lock()
	defer g.sseMu.Unlock()
	for ch := range g.sseClients {
		select {
		case ch <- event:
		default:
			// Skip slow clients
		}
	}
}

func (g *Gateway) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := make(chan ClusterEvent, 8)
	g.sseMu.Lock()
	g.sseClients[ch] = struct{}{}
	g.sseMu.Unlock()

	defer func() {
		g.sseMu.Lock()
		delete(g.sseClients, ch)
		g.sseMu.Unlock()
		close(ch)
	}()

	// Send current state immediately on connect
	g.mu.RLock()
	current := g.overview
	g.mu.RUnlock()
	sendSSEEvent(w, flusher, ClusterEvent{
		Type:      "state_update",
		Payload:   current,
		Timestamp: time.Now(),
	})

	// Heartbeat ticker
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			sendSSEEvent(w, flusher, event)
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func sendSSEEvent(w http.ResponseWriter, flusher http.Flusher, event ClusterEvent) {
	data, _ := json.Marshal(event)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, string(data))
	flusher.Flush()
}

// ---- HTTP Handlers ----

func (g *Gateway) handleOverview(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	overview := g.overview
	g.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(overview)
}

func (g *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"healthy","service":"raft-kv-gateway"}`))
}

func (g *Gateway) handleAggregateMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, addr := range g.cfg.NodeAddrs {
		resp, err := g.httpClient.Get("http://" + addr + "/metrics")
		if err != nil {
			fmt.Fprintf(w, "# ERROR fetching metrics from %s: %v\n", addr, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		// Label each metric line with the source node address
		lines := strings.Split(string(body), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "#") || line == "" {
				fmt.Fprintln(w, line)
			} else {
				// Insert node label
				nodeLabel := fmt.Sprintf(`node="%s"`, addr)
				if idx := strings.Index(line, "{"); idx != -1 {
					// Has existing labels
					fmt.Fprintf(w, "%s,%s%s\n", line[:idx+1], nodeLabel, line[idx+1:])
				} else if idx := strings.Index(line, " "); idx != -1 {
					// No labels — add them
					fmt.Fprintf(w, "%s{%s} %s\n", line[:idx], nodeLabel, line[idx+1:])
				}
			}
		}
	}
}

func (g *Gateway) handleKVProxy(w http.ResponseWriter, r *http.Request) {
	// Auth check for writes
	if r.Method != http.MethodGet {
		if !g.authenticate(w, r) {
			return
		}
	}

	g.mu.RLock()
	leaderAddr := g.leaderAddr
	g.mu.RUnlock()

	if leaderAddr == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"no leader elected, cluster may be in election"}`))
		return
	}

	g.proxyRequest(w, r, leaderAddr)
}

func (g *Gateway) handleAdminProxy(w http.ResponseWriter, r *http.Request) {
	if !g.authenticate(w, r) {
		return
	}

	g.mu.RLock()
	leaderAddr := g.leaderAddr
	g.mu.RUnlock()

	if leaderAddr == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"no leader"}`))
		return
	}

	g.proxyRequest(w, r, leaderAddr)
}

// proxyRequest forwards the incoming request to the target Raft node address.
func (g *Gateway) proxyRequest(w http.ResponseWriter, r *http.Request, nodeAddr string) {
	targetURL := &url.URL{
		Scheme:   "http",
		Host:     nodeAddr,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, `{"error":"failed to build proxy request"}`, http.StatusInternalServerError)
		return
	}

	// Copy relevant headers
	for _, h := range []string{"Content-Type", "X-Client-ID", "X-Request-ID"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}

	// Auto-inject dedup headers if not present
	if req.Header.Get("X-Client-ID") == "" {
		req.Header.Set("X-Client-ID", sessionID(r))
	}
	if req.Header.Get("X-Request-ID") == "" {
		req.Header.Set("X-Request-ID", fmt.Sprintf("%d", g.reqIDCounter.Add(1)))
	}

	// Set cluster auth token server-side
	if g.cfg.ClusterToken != "" {
		req.Header.Set("Authorization", "Bearer "+g.cfg.ClusterToken)
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		log.Printf("[GATEWAY] Proxy error to %s: %v", nodeAddr, err)
		// Try to find a new leader if this request failed
		go g.pollAllNodes()
		http.Error(w, `{"error":"upstream node unreachable"}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ---- Auth ----

func (g *Gateway) authenticate(w http.ResponseWriter, r *http.Request) bool {
	if g.cfg.GatewayToken == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"missing or invalid Authorization header"}`))
		return false
	}
	provided := strings.TrimPrefix(auth, "Bearer ")
	if provided != g.cfg.GatewayToken {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"invalid gateway token"}`))
		return false
	}
	return true
}

// ---- Helpers ----

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Client-ID, X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func sessionID(r *http.Request) string {
	// Use remote addr as a stable-ish session proxy; a real impl would use a cookie
	ip := r.RemoteAddr
	if idx := strings.LastIndex(ip, ":"); idx != -1 {
		ip = ip[:idx]
	}
	return fmt.Sprintf("gw-%s", ip)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
