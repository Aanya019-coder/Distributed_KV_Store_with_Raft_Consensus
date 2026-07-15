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
	MaxKeySize   = 1024      // 1KB
	MaxValueSize = 1048576   // 1MB
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
	allowUnauthenticatedReads bool
	peerHTTPAddrs             map[string]string // map peerID -> HTTP address (e.g. "localhost:8001")
	limiter                   *RateLimiter
	listener                  net.Listener
	httpServer                *http.Server
}

// NewServer creates a new API Server instance.
func NewServer(
	r *raft.Raft,
	authToken string,
	allowReadsUnauth bool,
	peerHTTPAddrs map[string]string,
) *Server {
	return &Server{
		raft:                      r,
		authToken:                 authToken,
		allowUnauthenticatedReads: allowReadsUnauth,
		peerHTTPAddrs:             peerHTTPAddrs,
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
	mux.HandleFunc("/kv/", s.handleKV)

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
		leaderAddr, ok := s.peerHTTPAddrs[leaderID]
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
		s.handleDelete(w, key)
	default:
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, key string) {
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
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxValueSize+1))
	if err != nil {
		http.Error(w, `{"error": "failed to read body"}`, http.StatusBadRequest)
		return
	}

	if len(body) > MaxValueSize {
		http.Error(w, `{"error": "value size exceeds maximum allowed size"}`, http.StatusBadRequest)
		return
	}

	val := string(body)
	s.logSanitized("PUT", key, len(val))

	success, err := s.raft.Propose("PUT", key, val)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if !success {
		http.Error(w, `{"error": "propose failed to commit"}`, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "ok"}`))
}

func (s *Server) handleDelete(w http.ResponseWriter, key string) {
	s.logSanitized("DELETE", key, 0)

	success, err := s.raft.Propose("DEL", key, "")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if !success {
		http.Error(w, `{"error": "propose failed to commit"}`, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "ok"}`))
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

func (s *Server) logSanitized(op, key string, size int) {
	hash := sha256.Sum256([]byte(key))
	fmt.Printf("[INFO] API Request: %s key_name=%s key_hash=%x val_size=%d bytes\n", op, key, hash[:8], size)
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
