package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8001", "Target HTTP API base URL")
	token := flag.String("token", "mysecrettoken", "Authorization Bearer token")
	concurrency := flag.Int("concurrency", 50, "Number of concurrent workers")
	durationSec := flag.Int("duration", 5, "Duration of test in seconds")
	op := flag.String("op", "PUT", "Operation to benchmark: PUT, GET, or MIXED")
	flag.Parse()

	fmt.Printf("Starting Raft KV Store Benchmark...\n")
	fmt.Printf("Target: %s | Concurrency: %d | Duration: %ds | Operation: %s\n\n", *url, *concurrency, *durationSec, *op)

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
		},
	}

	var totalOps int64
	var totalErr int64
	latenciesLock := sync.Mutex{}
	var latencies []float64

	stopCh := make(chan struct{})
	go func() {
		time.Sleep(time.Duration(*durationSec) * time.Second)
		close(stopCh)
	}()

	startBenchmark := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			reqID := int64(1)
			clientID := fmt.Sprintf("bench-client-%d", workerID)

			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("bench_key_%d", rand.Intn(1000))
					var req *http.Request
					var err error

					opType := *op
					if opType == "MIXED" {
						if rand.Float32() < 0.5 {
							opType = "PUT"
						} else {
							opType = "GET"
						}
					}

					reqURL := fmt.Sprintf("%s/kv/%s", *url, key)
					opStart := time.Now()

					if opType == "PUT" {
						val := fmt.Sprintf("payload_%d", rand.Intn(10000))
						body := bytes.NewBufferString(val)
						req, err = http.NewRequest(http.MethodPut, reqURL, body)
						if req != nil {
							req.Header.Set("X-Client-ID", clientID)
							req.Header.Set("X-Request-ID", fmt.Sprintf("%d", reqID))
						}
						reqID++
					} else {
						req, err = http.NewRequest(http.MethodGet, reqURL, nil)
					}

					if err != nil {
						atomic.AddInt64(&totalErr, 1)
						continue
					}

					if *token != "" {
						req.Header.Set("Authorization", "Bearer "+*token)
					}

					resp, err := client.Do(req)
					elapsed := time.Since(opStart).Seconds() * 1000.0 // ms

					if err != nil || (resp != nil && resp.StatusCode >= 400) {
						atomic.AddInt64(&totalErr, 1)
					} else {
						atomic.AddInt64(&totalOps, 1)
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
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(startBenchmark).Seconds()

	if len(latencies) == 0 {
		fmt.Printf("Benchmark finished: 0 successful operations (Errors: %d). Is the cluster running?\n", totalErr)
		return
	}

	sort.Float64s(latencies)
	n := float64(len(latencies))
	p50 := latencies[int(n*0.50)]
	p95 := latencies[int(n*0.95)]
	p99 := latencies[int(n*0.99)]
	throughput := float64(totalOps) / totalDuration

	fmt.Printf("==================== BENCHMARK RESULTS ====================\n")
	fmt.Printf("Operation          : %s\n", *op)
	fmt.Printf("Concurrency        : %d\n", *concurrency)
	fmt.Printf("Total Operations   : %d\n", totalOps)
	fmt.Printf("Total Errors       : %d\n", totalErr)
	fmt.Printf("Throughput (req/s) : %.2f\n", throughput)
	fmt.Printf("p50 Latency        : %.2f ms\n", p50)
	fmt.Printf("p95 Latency        : %.2f ms\n", p95)
	fmt.Printf("p99 Latency        : %.2f ms\n", p99)
	fmt.Printf("===========================================================\n")
}
