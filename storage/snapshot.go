package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ClusterConfigSnapshot is the serializable form of cluster configuration
// persisted in snapshots so config survives log compaction.
type ClusterConfigSnapshot struct {
	OldPeers map[string]string `json:"old_peers,omitempty"`
	NewPeers map[string]string `json:"new_peers"`
	OldHTTP  map[string]string `json:"old_http,omitempty"`
	NewHTTP  map[string]string `json:"new_http"`
	Joint    bool              `json:"joint"`
}

// DedupEntry stores the last applied request ID and cached response per client for deduplication.
type DedupEntry struct {
	LastAppliedRequestID int64  `json:"last_applied_request_id"`
	Response             string `json:"response"`
	Status               string `json:"status"`
}

// Snapshot represents a point-in-time state of the key-value store and the consensus metadata.
type Snapshot struct {
	LastIncludedIndex int64                  `json:"last_included_index"`
	LastIncludedTerm  int64                  `json:"last_included_term"`
	KVState           map[string]string      `json:"kv_state"`
	ClusterConfig     *ClusterConfigSnapshot `json:"cluster_config,omitempty"`
	DedupTable        map[string]DedupEntry  `json:"dedup_table,omitempty"`
}

// SaveSnapshot writes the snapshot data atomically to disk with 0600 permissions.
func SaveSnapshot(path string, snap *Snapshot) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open temp snapshot file: %w", err)
	}
	defer func() {
		if file != nil {
			file.Close()
			os.Remove(tmpPath)
		}
	}()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(snap); err != nil {
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot file: %w", err)
	}
	file = nil

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to commit snapshot file atomically: %w", err)
	}

	return nil
}

// LoadSnapshot reads and decodes the snapshot from disk.
func LoadSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()

	var snap Snapshot
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&snap); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}

	return &snap, nil
}
