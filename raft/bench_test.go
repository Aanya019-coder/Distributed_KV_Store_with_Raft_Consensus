package raft

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type benchCluster struct {
	tc        *testCluster
	httpAddrs map[string]string
	servers   map[string]*http.Server
	listeners map[string]net.Listener
}

func newBenchCluster(t *testing.T, count int) *benchCluster {
	tc := newTestCluster(t, count)

	httpAddrs := make(map[string]string)
	listeners := make(map[string]net.Listener)
	servers := make(map[string]*http.Server)

	portBase := getNextPortBase(count)
	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("node%d", i)
		httpAddrs[id] = fmt.Sprintf("127.0.0.1:%d", portBase+i)
	}

	// Set peer HTTP addrs and high snapshot threshold on nodes
	for id, r := range tc.nodes {
		r.snapshotThreshold = 100000
		peerHTTP := make(map[string]string)
		for pid, paddr := range httpAddrs {
			if pid != id {
				peerHTTP[pid] = paddr
			}
		}
		r.SetPeerHTTPAddrs(peerHTTP)
	}

	for id, addr := range httpAddrs {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("failed to listen on http %s: %v", addr, err)
		}
		listeners[id] = lis

		r := tc.nodes[id]
		mux := http.NewServeMux()
		mux.HandleFunc("/kv/", func(w http.ResponseWriter, req *http.Request) {
			key := req.URL.Path[4:]
			if req.Method == http.MethodPut {
				body, _ := io.ReadAll(req.Body)
				var reqID int64
				fmt.Sscanf(req.Header.Get("X-Request-ID"), "%d", &reqID)
				ok, _ := r.ProposeWithClient("PUT", key, string(body), req.Header.Get("X-Client-ID"), reqID)
				if ok {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"status":"ok"}`))
				} else {
					http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
				}
			} else if req.Method == http.MethodGet {
				val, ok, isL := r.Get(key)
				if !isL {
					http.Error(w, `{"error":"not leader"}`, http.StatusServiceUnavailable)
					return
				}
				if !ok {
					http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprintf(w, `{"key":"%s","value":"%s"}`, key, val)
			}
		})

		srv := &http.Server{Handler: mux}
		servers[id] = srv
		go srv.Serve(lis)
	}

	return &benchCluster{
		tc:        tc,
		httpAddrs: httpAddrs,
		servers:   servers,
		listeners: listeners,
	}
}

func (bc *benchCluster) stop() {
	for _, srv := range bc.servers {
		srv.Close()
	}
	bc.tc.stop()
}

func TestPerformanceBenchmark(t *testing.T) {
	bc := newBenchCluster(t, 3)
	defer bc.stop()

	// Wait for leader
	var leaderID string
	var leaderNode *Raft
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		for id, n := range bc.tc.nodes {
			if isL, _ := n.IsLeader(); isL {
				leaderID = id
				leaderNode = n
				break
			}
		}
		if leaderNode != nil {
			break
		}
	}

	if leaderNode == nil {
		t.Fatalf("failed to elect leader")
	}

	leaderHTTP := bc.httpAddrs[leaderID]
	t.Logf("Leader HTTP endpoint: http://%s", leaderHTTP)

	// Warmup write
	leaderNode.Propose("PUT", "warmup_key", "warmup_val")

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
		},
	}

	runBench := func(op string, concurrency int, durationSec int) (float64, float64, float64, float64) {
		var ops int64
		var errs int64
		latenciesLock := sync.Mutex{}
		var latencies []float64

		stopCh := make(chan struct{})
		go func() {
			time.Sleep(time.Duration(durationSec) * time.Second)
			close(stopCh)
		}()

		start := time.Now()
		var wg sync.WaitGroup

		for c := 0; c < concurrency; c++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				reqID := int64(1)
				clientID := fmt.Sprintf("bench-%s-%d-%d", op, concurrency, workerID)

				for {
					select {
					case <-stopCh:
						return
					default:
						key := fmt.Sprintf("key_%d", rand.Intn(100))
						opStart := time.Now()

						var req *http.Request
						if op == "PUT" {
							val := fmt.Sprintf("val_%d", rand.Intn(1000))
							req, _ = http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/kv/%s", leaderHTTP, key), bytes.NewBufferString(val))
							req.Header.Set("X-Client-ID", clientID)
							req.Header.Set("X-Request-ID", fmt.Sprintf("%d", reqID))
							reqID++
						} else {
							req, _ = http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/kv/%s", leaderHTTP, key), nil)
						}

						resp, err := client.Do(req)
						elapsed := time.Since(opStart).Seconds() * 1000.0 // ms

						isSuccess := (err == nil && resp != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound))

						if !isSuccess {
							atomic.AddInt64(&errs, 1)
						} else {
							atomic.AddInt64(&ops, 1)
							latenciesLock.Lock()
							latencies = append(latencies, elapsed)
							latenciesLock.Unlock()
						}

						if resp != nil {
							io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
						}
					}
				}
			}(c)
		}

		wg.Wait()
		durSec := time.Since(start).Seconds()

		if len(latencies) == 0 {
			return 0, 0, 0, 0
		}

		sort.Float64s(latencies)
		n := float64(len(latencies))
		p50 := latencies[int(n*0.50)]
		p95 := latencies[int(n*0.95)]
		p99 := latencies[int(n*0.99)]
		tp := float64(ops) / durSec

		return tp, p50, p95, p99
	}

	t.Logf("\n=== BENCHMARK RESULTS (Local 3-Node Raft Cluster) ===")
	t.Logf("%-10s | %-11s | %-18s | %-8s | %-8s | %-8s", "Operation", "Concurrency", "Throughput (req/s)", "p50", "p95", "p99")
	t.Logf("-------------------------------------------------------------------------")

	concurrencies := []int{10, 50, 100}
	ops := []string{"PUT", "GET"}

	for _, op := range ops {
		for _, conc := range concurrencies {
			tp, p50, p95, p99 := runBench(op, conc, 2)
			t.Logf("%-10s | %-11d | %-18.2f | %-6.2fms | %-6.2fms | %-6.2fms", op, conc, tp, p50, p95, p99)
		}
	}
}
