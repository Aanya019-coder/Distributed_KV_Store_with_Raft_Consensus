package raft

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"raft-kv/storage"
)

func init() {
	storage.FsyncObserver = func(durationMs float64) {
		WALFsyncDurationMs.Observe(durationMs)
	}
}

// ---- Lightweight Prometheus-compatible metrics ----
//
// This implements a minimal, zero-dependency metrics system that emits
// Prometheus text exposition format on a /metrics endpoint.
// This avoids adding github.com/prometheus/client_golang as a dependency
// while still being fully scrapeable by any Prometheus server.

// MetricType defines the Prometheus metric type.
type MetricType int

const (
	MetricGauge MetricType = iota
	MetricCounter
	MetricHistogram
)

// Gauge is a metric that can go up or down.
type Gauge struct {
	mu    sync.Mutex
	name  string
	help  string
	value float64
}

func NewGauge(name, help string) *Gauge {
	g := &Gauge{name: name, help: help}
	globalRegistry.register(g)
	return g
}

func (g *Gauge) Set(v float64) {
	g.mu.Lock()
	g.value = v
	g.mu.Unlock()
}

func (g *Gauge) serialize() string {
	g.mu.Lock()
	v := g.value
	g.mu.Unlock()
	return "# HELP " + g.name + " " + g.help + "\n" +
		"# TYPE " + g.name + " gauge\n" +
		g.name + " " + strconv.FormatFloat(v, 'f', -1, 64) + "\n"
}

// Counter is a monotonically increasing metric.
type Counter struct {
	mu    sync.Mutex
	name  string
	help  string
	value float64
}

func NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	globalRegistry.register(c)
	return c
}

func (c *Counter) Inc() {
	c.mu.Lock()
	c.value++
	c.mu.Unlock()
}

func (c *Counter) Add(v float64) {
	c.mu.Lock()
	c.value += v
	c.mu.Unlock()
}

func (c *Counter) serialize() string {
	c.mu.Lock()
	v := c.value
	c.mu.Unlock()
	return "# HELP " + c.name + " " + c.help + "\n" +
		"# TYPE " + c.name + " counter\n" +
		c.name + " " + strconv.FormatFloat(v, 'f', -1, 64) + "\n"
}

// LabeledCounter tracks counts by label values.
type LabeledCounter struct {
	mu     sync.Mutex
	name   string
	help   string
	labels []string
	values map[string]float64 // key = joined label values
}

func NewLabeledCounter(name, help string, labels []string) *LabeledCounter {
	lc := &LabeledCounter{
		name:   name,
		help:   help,
		labels: labels,
		values: make(map[string]float64),
	}
	globalRegistry.register(lc)
	return lc
}

func (lc *LabeledCounter) Inc(labelValues ...string) {
	key := joinLabels(labelValues)
	lc.mu.Lock()
	lc.values[key]++
	lc.mu.Unlock()
}

func (lc *LabeledCounter) serialize() string {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	result := "# HELP " + lc.name + " " + lc.help + "\n" +
		"# TYPE " + lc.name + " counter\n"
	for key, val := range lc.values {
		labelStr := formatLabels(lc.labels, splitLabels(key))
		result += lc.name + "{" + labelStr + "} " + strconv.FormatFloat(val, 'f', -1, 64) + "\n"
	}
	return result
}

// Histogram tracks the distribution of observed values.
type Histogram struct {
	mu      sync.Mutex
	name    string
	help    string
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

func NewHistogram(name, help string, buckets []float64) *Histogram {
	h := &Histogram{
		name:    name,
		help:    help,
		buckets: buckets,
		counts:  make([]uint64, len(buckets)),
	}
	globalRegistry.register(h)
	return h
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	h.sum += v
	h.count++
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
	h.mu.Unlock()
}

func (h *Histogram) serialize() string {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := "# HELP " + h.name + " " + h.help + "\n" +
		"# TYPE " + h.name + " histogram\n"

	cumulative := uint64(0)
	for i, b := range h.buckets {
		cumulative += h.counts[i]
		result += h.name + "_bucket{le=\"" + strconv.FormatFloat(b, 'f', -1, 64) + "\"} " +
			strconv.FormatUint(cumulative, 10) + "\n"
	}
	result += h.name + "_bucket{le=\"+Inf\"} " + strconv.FormatUint(h.count, 10) + "\n"
	result += h.name + "_sum " + strconv.FormatFloat(h.sum, 'f', -1, 64) + "\n"
	result += h.name + "_count " + strconv.FormatUint(h.count, 10) + "\n"
	return result
}

// LabeledHistogram tracks histograms by label values.
type LabeledHistogram struct {
	mu      sync.Mutex
	name    string
	help    string
	labels  []string
	buckets []float64
	hists   map[string]*Histogram
}

func NewLabeledHistogram(name, help string, labels []string, buckets []float64) *LabeledHistogram {
	lh := &LabeledHistogram{
		name:    name,
		help:    help,
		labels:  labels,
		buckets: buckets,
		hists:   make(map[string]*Histogram),
	}
	globalRegistry.register(lh)
	return lh
}

func (lh *LabeledHistogram) Observe(value float64, labelValues ...string) {
	key := joinLabels(labelValues)
	lh.mu.Lock()
	h, ok := lh.hists[key]
	if !ok {
		h = &Histogram{
			name:    lh.name,
			help:    lh.help,
			buckets: lh.buckets,
			counts:  make([]uint64, len(lh.buckets)),
		}
		lh.hists[key] = h
	}
	lh.mu.Unlock()
	h.Observe(value)
}

func (lh *LabeledHistogram) serialize() string {
	lh.mu.Lock()
	defer lh.mu.Unlock()

	result := "# HELP " + lh.name + " " + lh.help + "\n" +
		"# TYPE " + lh.name + " histogram\n"

	for key, h := range lh.hists {
		labelStr := formatLabels(lh.labels, splitLabels(key))
		h.mu.Lock()
		cumulative := uint64(0)
		for i, b := range h.buckets {
			cumulative += h.counts[i]
			result += lh.name + "_bucket{" + labelStr + ",le=\"" + strconv.FormatFloat(b, 'f', -1, 64) + "\"} " +
				strconv.FormatUint(cumulative, 10) + "\n"
		}
		result += lh.name + "_bucket{" + labelStr + ",le=\"+Inf\"} " + strconv.FormatUint(h.count, 10) + "\n"
		result += lh.name + "_sum{" + labelStr + "} " + strconv.FormatFloat(h.sum, 'f', -1, 64) + "\n"
		result += lh.name + "_count{" + labelStr + "} " + strconv.FormatUint(h.count, 10) + "\n"
		h.mu.Unlock()
	}
	return result
}

// ---- Global Registry ----

type serializable interface {
	serialize() string
}

type registry struct {
	mu      sync.Mutex
	metrics []serializable
}

var globalRegistry = &registry{}

func (r *registry) register(m serializable) {
	r.mu.Lock()
	r.metrics = append(r.metrics, m)
	r.mu.Unlock()
}

func (r *registry) serializeAll() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := ""
	for _, m := range r.metrics {
		result += m.serialize()
	}
	return result
}

// MetricsHandler returns an HTTP handler for the /metrics endpoint.
func MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(globalRegistry.serializeAll()))
	}
}

// ---- Raft Metrics Instances ----

var (
	RaftCurrentTerm         = NewGauge("raft_current_term", "Current Raft term")
	RaftLeaderElectionsTotal = NewCounter("raft_leader_elections_total", "Total leader elections")
	RaftCommitIndex         = NewGauge("raft_commit_index", "Current commit index")
	RaftLogEntriesTotal     = NewCounter("raft_log_entries_total", "Total log entries appended")
	RaftRole                = NewGauge("raft_role", "Node role (0=follower, 1=candidate, 2=leader)")
	RaftReplicationLagMs    = NewLabeledHistogram("raft_replication_lag_ms", "Replication lag in ms",
		[]string{"peer"}, []float64{1, 2, 5, 10, 25, 50, 100, 250, 500})
	KVRequestDurationMs = NewLabeledHistogram("kv_request_duration_ms", "Request latency in ms",
		[]string{"method"}, []float64{0.5, 1, 2, 5, 10, 25, 50, 100})
	KVRequestsTotal = NewLabeledCounter("kv_requests_total", "Total KV requests",
		[]string{"method", "status"})
	WALFsyncDurationMs = NewHistogram("wal_fsync_duration_ms", "WAL fsync latency in ms",
		[]float64{0.1, 0.5, 1, 2, 5, 10, 25})
)

// RecordRequestMetrics is a helper for the API layer to record request metrics.
func RecordRequestMetrics(method string, startTime time.Time, success bool) {
	durationMs := float64(time.Since(startTime).Microseconds()) / 1000.0
	KVRequestDurationMs.Observe(durationMs, method)

	status := "success"
	if !success {
		status = "error"
	}
	KVRequestsTotal.Inc(method, status)
}

// ---- Label helpers ----

func joinLabels(values []string) string {
	result := ""
	for i, v := range values {
		if i > 0 {
			result += "|"
		}
		result += v
	}
	return result
}

func splitLabels(key string) []string {
	if key == "" {
		return nil
	}
	result := []string{}
	current := ""
	for _, c := range key {
		if c == '|' {
			result = append(result, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	result = append(result, current)
	return result
}

func formatLabels(names, values []string) string {
	result := ""
	for i, name := range names {
		if i > 0 {
			result += ","
		}
		val := ""
		if i < len(values) {
			val = values[i]
		}
		result += name + "=\"" + val + "\""
	}
	return result
}
