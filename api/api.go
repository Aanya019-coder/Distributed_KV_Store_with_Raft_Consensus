package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"raft-kv/raft"
)

const (
	MaxKeySize   = 1024    // 1KB
	MaxValueSize = 1048576 // 1MB
)

// RateLimiter implements a simple thread-safe token-bucket rate limiter.
type RateLimiter struct {
	mu           sync.Mutex
	rate         float64 // tokens per second
	capacity     float64 // max tokens
	tokens       float64
	lastRefilled time.Time
}

// NewRateLimiter initializes a rate limiter.
func NewRateLimiter(rate, capacity float64) *RateLimiter {
	return &RateLimiter{
		rate:         rate,
		capacity:     capacity,
		tokens:       capacity,
		lastRefilled: time.Now(),
	}
}

// Allow returns true if a request is allowed.
func (l *RateLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastRefilled).Seconds()
	l.lastRefilled = now

	l.tokens += elapsed * l.rate
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}

	if l.tokens >= 1.0 {
		l.tokens -= 1.0
		return true
	}
	return false
}

// Server wrapper around HTTP KV endpoints.
type Server struct {
	raft                      *raft.Raft
	authToken                 string
	adminToken                string
	allowUnauthenticatedReads bool
	limiter                   *RateLimiter
	listener                  net.Listener
	httpServer                *http.Server
}

// NewServer creates a new API Server instance.
func NewServer(
	r *raft.Raft,
	authToken string,
	adminToken string,
	allowReadsUnauth bool,
) *Server {
	return &Server{
		raft:                      r,
		authToken:                 authToken,
		adminToken:                adminToken,
		allowUnauthenticatedReads: allowReadsUnauth,
		limiter:                   NewRateLimiter(50, 100), // 50 requests/sec, burst 100
	}
}

// Start starts the HTTP server.
func (s *Server) Start(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/admin/addnode", s.handleAddNode)
	mux.HandleFunc("/admin/removenode", s.handleRemoveNode)
	mux.HandleFunc("/metrics", raft.MetricsHandler())

	// Wrap in recovery middleware for resilience
	s.httpServer = &http.Server{
		Handler: s.recoveryMiddleware(mux),
	}

	go func() {
		if err := s.httpServer.Serve(l); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[ERROR] HTTP Server error: %v\n", err)
		}
	}()

	return nil
}

// Stop closes the server.
func (s *Server) Stop() error {
	if s.httpServer != nil {
		ctx, cancel := contextWithTimeout()
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// Address returns the actual server listening address.
func (s *Server) Address() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	// Rate Limiting Check
	if !s.limiter.Allow() {
		http.Error(w, `{"error": "too many requests, rate limit exceeded"}`, http.StatusTooManyRequests)
		return
	}

	// Read URL Key
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, `{"error": "key required"}`, http.StatusBadRequest)
		return
	}

	if len(key) > MaxKeySize {
		http.Error(w, `{"error": "key size exceeds maximum allowed size"}`, http.StatusBadRequest)
		return
	}

	// Auth Check
	if !s.authenticate(w, r) {
		return
	}

	// Check Leader redirection
	isLeader, leaderID := s.raft.IsLeader()
	if !isLeader {
		peerHTTPAddrs := s.raft.GetPeerHTTPAddrs()
		leaderAddr, ok := peerHTTPAddrs[leaderID]
		if ok && leaderAddr != "" {
			// Redirect client to leader
			url := fmt.Sprintf("http://%s/kv/%s", leaderAddr, key)
			w.Header().Set("Location", url)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, `{"error": "not the leader, leader election in progress"}`, http.StatusServiceUnavailable)
		return
	}

	// Execute operation based on HTTP Method
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func extractClientMetadata(r *http.Request, body []byte) (clientID string, requestID int64, value string) {
	clientID = r.Header.Get("X-Client-ID")
	if reqIDStr := r.Header.Get("X-Request-ID"); reqIDStr != "" {
		fmt.Sscanf(reqIDStr, "%d", &requestID)
	}

	if clientID == "" {
		clientID = r.URL.Query().Get("client_id")
	}
	if requestID == 0 {
		if reqIDStr := r.URL.Query().Get("request_id"); reqIDStr != "" {
			fmt.Sscanf(reqIDStr, "%d", &requestID)
		}
	}

	value = string(body)
	if len(body) > 0 {
		var jsonPayload struct {
			ClientID  string `json:"client_id"`
			RequestID int64  `json:"request_id"`
			Value     string `json:"value"`
			Val       string `json:"val"`
		}
		if err := json.Unmarshal(body, &jsonPayload); err == nil {
			if jsonPayload.ClientID != "" {
				clientID = jsonPayload.ClientID
			}
			if jsonPayload.RequestID > 0 {
				requestID = jsonPayload.RequestID
			}
			if jsonPayload.Value != "" || jsonPayload.Val != "" {
				if jsonPayload.Value != "" {
					value = jsonPayload.Value
				} else {
					value = jsonPayload.Val
				}
			}
		}
	}
	return clientID, requestID, value
}

func (s *Server) handleGet(w http.ResponseWriter, key string) {
	start := time.Now()
	success := false
	defer func() {
		raft.RecordRequestMetrics("GET", start, success)
	}()

	val, ok, isLeader := s.raft.Get(key)
	if !isLeader {
		http.Error(w, `{"error": "not the leader"}`, http.StatusServiceUnavailable)
		return
	}

	if !ok {
		http.Error(w, `{"error": "key not found"}`, http.StatusNotFound)
		return
	}

	s.logSanitized("GET", key, len(val))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
	success = true
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	start := time.Now()
	success := false
	defer func() {
		raft.RecordRequestMetrics("PUT", start, success)
	}()

	body, err := io.ReadAll(io.LimitReader(r.Body, MaxValueSize+1))
	if err != nil {
		http.Error(w, `{"error": "failed to read body"}`, http.StatusBadRequest)
		return
	}

	if len(body) > MaxValueSize {
		http.Error(w, `{"error": "value size exceeds maximum allowed size"}`, http.StatusBadRequest)
		return
	}

	clientID, requestID, val := extractClientMetadata(r, body)
	s.logSanitized("PUT", key, len(val))

	ok, err := s.raft.ProposeWithClient("PUT", key, val, clientID, requestID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if !ok {
		http.Error(w, `{"error": "propose failed to commit"}`, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "ok"}`))
	success = true
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	start := time.Now()
	success := false
	defer func() {
		raft.RecordRequestMetrics("DELETE", start, success)
	}()

	body, _ := io.ReadAll(io.LimitReader(r.Body, MaxValueSize+1))
	clientID, requestID, _ := extractClientMetadata(r, body)

	s.logSanitized("DELETE", key, 0)

	ok, err := s.raft.ProposeWithClient("DEL", key, "", clientID, requestID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if !ok {
		http.Error(w, `{"error": "propose failed to commit"}`, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "ok"}`))
	success = true
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"service":     "Distributed Key-Value Store with Raft Consensus",
		"node_id":     s.raft.GetID(),
		"status_url":  "/status",
		"metrics_url": "/metrics",
		"health_url":  "/healthz",
		"kv_api":      "/kv/{key}",
	})
}

// handleStatus returns the current cluster status. Public endpoint — no auth required.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	status := s.raft.GetStatus()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

// handleAddNode adds a node to the cluster via joint consensus.
func (s *Server) handleAddNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if !s.authenticateAdmin(w, r) {
		return
	}

	var req struct {
		ID       string `json:"id"`
		GRPCAddr string `json:"grpc_addr"`
		HTTPAddr string `json:"http_addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}

	if req.ID == "" || req.GRPCAddr == "" {
		http.Error(w, `{"error": "id and grpc_addr are required"}`, http.StatusBadRequest)
		return
	}

	success, err := s.raft.ProposeConfigChange(raft.ConfigChangeRequest{
		NodeID:   req.ID,
		GRPCAddr: req.GRPCAddr,
		HTTPAddr: req.HTTPAddr,
		Remove:   false,
	})

	if err != nil {
		// Check if it's a "not the leader" error and redirect
		isLeader, leaderID := s.raft.IsLeader()
		if !isLeader {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":   false,
				"error":     "not the leader",
				"leader_id": leaderID,
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": success,
	})
}

// handleRemoveNode removes a node from the cluster via joint consensus.
func (s *Server) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if !s.authenticateAdmin(w, r) {
		return
	}

	var req struct {
		ID string `json:"id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		http.Error(w, `{"error": "id is required"}`, http.StatusBadRequest)
		return
	}

	success, err := s.raft.ProposeConfigChange(raft.ConfigChangeRequest{
		NodeID: req.ID,
		Remove: true,
	})

	if err != nil {
		isLeader, leaderID := s.raft.IsLeader()
		if !isLeader {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":   false,
				"error":     "not the leader",
				"leader_id": leaderID,
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": success,
	})
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet && s.allowUnauthenticatedReads {
		return true
	}

	if s.authToken == "" {
		return true
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, `{"error": "unauthorized, missing authorization header"}`, http.StatusUnauthorized)
		return false
	}

	const prefix = "Bearer "
	if !strings.HasPrefix(authHeader, prefix) {
		http.Error(w, `{"error": "unauthorized, invalid authorization format"}`, http.StatusUnauthorized)
		return false
	}

	token := strings.TrimPrefix(authHeader, prefix)
	if token != s.authToken {
		http.Error(w, `{"error": "unauthorized, invalid token"}`, http.StatusUnauthorized)
		return false
	}

	return true
}

// authenticateAdmin checks the admin token for config-change endpoints.
// Falls back to the regular auth token if no separate admin token is set.
func (s *Server) authenticateAdmin(w http.ResponseWriter, r *http.Request) bool {
	token := s.adminToken
	if token == "" {
		token = s.authToken
	}
	if token == "" {
		return true
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, `{"error": "unauthorized, missing authorization header"}`, http.StatusUnauthorized)
		return false
	}

	const prefix = "Bearer "
	if !strings.HasPrefix(authHeader, prefix) {
		http.Error(w, `{"error": "unauthorized, invalid authorization format"}`, http.StatusUnauthorized)
		return false
	}

	provided := strings.TrimPrefix(authHeader, prefix)
	if provided != token {
		http.Error(w, `{"error": "unauthorized, invalid admin token"}`, http.StatusUnauthorized)
		return false
	}

	return true
}

func (s *Server) logSanitized(op, key string, size int) {
	hash := sha256.Sum256([]byte(key))
	fmt.Printf("[INFO] API Request: %s key_name=%s key_hash=%x val_size=%d bytes\n", op, key, hash[:8], size)
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Client-ID, X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		defer func() {
			if err := recover(); err != nil {
				fmt.Printf("[ERROR] Recovered from HTTP handler panic: %v\n", err)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error": "internal server error"}`))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func contextWithTimeout() (context.Context, context.CancelFunc) {
	return func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		t := time.NewTimer(timeout)
		ctx, cancel := context.WithCancel(parent)
		go func() {
			<-t.C
			cancel()
		}()
		return ctx, cancel
	}(context.Background(), 5*time.Second)
}

// handleHealthz responds to lightweight HTTP health checks.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "healthy"}`))
}
