package raft

import (
	"encoding/json"
	"fmt"
)

// EntryType distinguishes between regular commands and configuration changes.
type EntryType int

const (
	EntryCommand      EntryType = 0
	EntryConfigChange EntryType = 1
	EntryNoop         EntryType = 2
)

// ClusterConfig represents a cluster membership configuration.
// During joint consensus, both OldPeers and NewPeers are populated and Joint is true.
// In steady state, only NewPeers is populated and Joint is false.
type ClusterConfig struct {
	OldPeers    map[string]string `json:"old_peers,omitempty"`    // C_old gRPC addresses (empty in steady state)
	NewPeers    map[string]string `json:"new_peers"`              // C_new gRPC addresses
	OldHTTP     map[string]string `json:"old_http,omitempty"`     // C_old HTTP addresses
	NewHTTP     map[string]string `json:"new_http"`               // C_new HTTP addresses
	Joint       bool              `json:"joint"`                  // true during joint consensus phase
}

// ConfigChangeRequest is the payload serialized into a log entry's Command bytes
// when EntryType == EntryConfigChange.
type ConfigChangeRequest struct {
	NodeID   string `json:"node_id"`
	GRPCAddr string `json:"grpc_addr,omitempty"` // for add
	HTTPAddr string `json:"http_addr,omitempty"` // for add
	Remove   bool   `json:"remove"`
}

// LogEntryMeta carries entry type and config metadata alongside the raw LogEntry.
// Stored in the Command bytes of the protobuf LogEntry as a JSON envelope.
type LogEntryEnvelope struct {
	Type   EntryType          `json:"type"`
	Data   json.RawMessage    `json:"data,omitempty"`   // original command bytes for COMMAND type
	Config *ClusterConfig     `json:"config,omitempty"` // populated for CONFIG_CHANGE type
}

// IsJoint returns true if this config represents a joint consensus state.
func (c *ClusterConfig) IsJoint() bool {
	return c != nil && c.Joint
}

// ContainsNode checks if the given node ID is present in the effective configuration.
func (c *ClusterConfig) ContainsNode(nodeID string) bool {
	if c == nil {
		return false
	}
	if _, ok := c.NewPeers[nodeID]; ok {
		return true
	}
	if c.Joint {
		if _, ok := c.OldPeers[nodeID]; ok {
			return true
		}
	}
	return false
}

// AllNodeGRPC returns the union of all node gRPC addresses across both configs.
// The returned map excludes the given selfID.
func (c *ClusterConfig) AllNodeGRPC(selfID string) map[string]string {
	result := make(map[string]string)
	if c == nil {
		return result
	}
	for id, addr := range c.NewPeers {
		if id != selfID {
			result[id] = addr
		}
	}
	if c.Joint {
		for id, addr := range c.OldPeers {
			if id != selfID {
				if _, exists := result[id]; !exists {
					result[id] = addr
				}
			}
		}
	}
	return result
}

// AllNodeHTTP returns the union of all node HTTP addresses across both configs.
// The returned map excludes the given selfID.
func (c *ClusterConfig) AllNodeHTTP(selfID string) map[string]string {
	result := make(map[string]string)
	if c == nil {
		return result
	}
	for id, addr := range c.NewHTTP {
		if id != selfID {
			result[id] = addr
		}
	}
	if c.Joint {
		for id, addr := range c.OldHTTP {
			if id != selfID {
				if _, exists := result[id]; !exists {
					result[id] = addr
				}
			}
		}
	}
	return result
}

// HasMajority checks whether the given set of voter IDs (including self)
// constitutes a majority under this configuration.
// During joint consensus, requires majority in BOTH C_old and C_new independently.
func (c *ClusterConfig) HasMajority(selfID string, voters map[string]bool) bool {
	if c == nil {
		return false
	}

	// Check majority in C_new (always required)
	newCount := 0
	newTotal := len(c.NewPeers)
	for id := range c.NewPeers {
		if voters[id] {
			newCount++
		}
	}
	// Count self if in C_new
	if _, inNew := c.NewPeers[selfID]; inNew {
		if voters[selfID] {
			newCount++
		}
		newTotal++ // self is part of the cluster
	}

	if newCount < newTotal/2+1 {
		return false
	}

	// If not joint, C_new majority is sufficient
	if !c.Joint {
		return true
	}

	// Check majority in C_old as well
	oldCount := 0
	oldTotal := len(c.OldPeers)
	for id := range c.OldPeers {
		if voters[id] {
			oldCount++
		}
	}
	// Count self if in C_old
	if _, inOld := c.OldPeers[selfID]; inOld {
		if voters[selfID] {
			oldCount++
		}
		oldTotal++
	}

	return oldCount >= oldTotal/2+1
}

// VoterCount returns the total number of unique voters (including self) in this config.
func (c *ClusterConfig) VoterCount(selfID string) int {
	if c == nil {
		return 1
	}
	seen := make(map[string]bool)
	seen[selfID] = true
	for id := range c.NewPeers {
		seen[id] = true
	}
	if c.Joint {
		for id := range c.OldPeers {
			seen[id] = true
		}
	}
	return len(seen)
}

// BuildJointConfig creates a C_old,new joint configuration from the current
// steady-state config and the proposed change.
func BuildJointConfig(current *ClusterConfig, selfID string, req ConfigChangeRequest) (*ClusterConfig, error) {
	if current == nil {
		return nil, fmt.Errorf("cannot build joint config from nil current config")
	}
	if current.Joint {
		return nil, fmt.Errorf("config change already in flight (joint consensus active)")
	}

	// C_old = current config (NewPeers becomes OldPeers)
	oldPeers := copyMap(current.NewPeers)
	oldHTTP := copyMap(current.NewHTTP)

	// C_new = modified config
	newPeers := copyMap(current.NewPeers)
	newHTTP := copyMap(current.NewHTTP)

	if req.Remove {
		if _, exists := newPeers[req.NodeID]; !exists {
			// Check if it's self (self isn't in its own peers map)
			if req.NodeID != selfID {
				return nil, fmt.Errorf("node %s not found in current configuration", req.NodeID)
			}
		}
		delete(newPeers, req.NodeID)
		delete(newHTTP, req.NodeID)
	} else {
		if _, exists := newPeers[req.NodeID]; exists {
			return nil, fmt.Errorf("node %s already in configuration", req.NodeID)
		}
		if req.NodeID == selfID {
			return nil, fmt.Errorf("cannot add self to configuration")
		}
		newPeers[req.NodeID] = req.GRPCAddr
		newHTTP[req.NodeID] = req.HTTPAddr
	}

	return &ClusterConfig{
		OldPeers: oldPeers,
		NewPeers: newPeers,
		OldHTTP:  oldHTTP,
		NewHTTP:  newHTTP,
		Joint:    true,
	}, nil
}

// BuildFinalConfig creates the final C_new configuration from a joint config.
func BuildFinalConfig(joint *ClusterConfig) *ClusterConfig {
	if joint == nil {
		return nil
	}
	return &ClusterConfig{
		OldPeers: nil,
		NewPeers: copyMap(joint.NewPeers),
		OldHTTP:  nil,
		NewHTTP:  copyMap(joint.NewHTTP),
		Joint:    false,
	}
}

// ConfigFromPeers builds an initial steady-state ClusterConfig from the legacy
// peer maps used in the original codebase.
func ConfigFromPeers(grpcPeers, httpPeers map[string]string, selfID string) *ClusterConfig {
	newPeers := copyMap(grpcPeers)
	newHTTP := copyMap(httpPeers)
	newPeers[selfID] = "localhost:self"
	newHTTP[selfID] = "localhost:self"
	return &ClusterConfig{
		NewPeers: newPeers,
		NewHTTP:  newHTTP,
		Joint:    false,
	}
}

// EncodeConfigChangeEntry serializes a config-change log entry payload.
func EncodeConfigChangeEntry(config *ClusterConfig) ([]byte, error) {
	env := LogEntryEnvelope{
		Type:   EntryConfigChange,
		Config: config,
	}
	return json.Marshal(env)
}

// EncodeNoopEntry serializes a no-op log entry payload.
func EncodeNoopEntry() ([]byte, error) {
	env := LogEntryEnvelope{
		Type: EntryNoop,
	}
	return json.Marshal(env)
}

// EncodeCommandEntry wraps a regular command in the envelope format.
func EncodeCommandEntry(cmdBytes []byte) ([]byte, error) {
	env := LogEntryEnvelope{
		Type: EntryCommand,
		Data: cmdBytes,
	}
	return json.Marshal(env)
}

// DecodeLogEntry decodes the envelope from log entry command bytes.
// Returns EntryCommand with the original data for backward-compatible raw command entries.
func DecodeLogEntry(cmdBytes []byte) (*LogEntryEnvelope, error) {
	if len(cmdBytes) == 0 {
		return &LogEntryEnvelope{Type: EntryNoop}, nil
	}

	var env LogEntryEnvelope
	if err := json.Unmarshal(cmdBytes, &env); err != nil {
		// Backward compatibility: if it doesn't decode as an envelope,
		// treat it as a raw command (pre-Phase-2 entries).
		return &LogEntryEnvelope{
			Type: EntryCommand,
			Data: cmdBytes,
		}, nil
	}

	// If type is zero and there's no config, check if it might be a raw command
	// that happens to be valid JSON (e.g., {"op":"PUT","key":"k","val":"v"}).
	if env.Type == EntryCommand && env.Config == nil && env.Data == nil {
		// This is likely a raw command that was parsed as an envelope
		// but didn't have the envelope fields. Treat as raw command.
		return &LogEntryEnvelope{
			Type: EntryCommand,
			Data: cmdBytes,
		}, nil
	}

	return &env, nil
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
