package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftproto "raft-kv/proto"
	"raft-kv/storage"
	"raft-kv/transport"
)

// RaftState defines the current consensus state.
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Shutdown"
	}
}

// Command is the operation applied to the KV store.
type Command struct {
	Op  string `json:"op"`
	Key string `json:"key"`
	Val string `json:"val"`
}

type rpcReq struct {
	req  interface{}
	resp chan interface{}
}

type cmdReq struct {
	cmd  []byte
	resp chan cmdResp
}

type cmdResp struct {
	success bool
	err     error
}

type voteRespMsg struct {
	peerID string
	resp   *raftproto.RequestVoteResponse
}

type appendRespMsg struct {
	peerID string
	resp   *raftproto.AppendEntriesResponse
}

type snapshotRespMsg struct {
	peerID string
	resp   *raftproto.InstallSnapshotResponse
}

type peerClient struct {
	cli  raftproto.RaftServiceClient
	conn *grpc.ClientConn
}

type proposal struct {
	index  int64
	term   int64
	respCh chan cmdResp
}

// Raft implements the Raft consensus state machine.
type Raft struct {
	raftproto.UnimplementedRaftServiceServer

	mu        sync.Mutex
	id        string
	peers     map[string]string
	grpcPeers map[string]*peerClient

	state             RaftState
	currentTerm       int64
	votedFor          string
	log               []*raftproto.LogEntry
	votesReceived     map[string]bool
	proposals         []*proposal
	lastIncludedIndex int64
	lastIncludedTerm  int64

	commitIndex int64
	lastApplied int64

	nextIndex  map[string]int64
	matchIndex map[string]int64

	rpcCh    chan rpcReq
	cmdCh    chan cmdReq
	shutdown chan struct{}

	electionTimeout time.Duration
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	storageDir   string
	wal          *storage.WAL
	snapshotPath string
	kv           map[string]string
	snapshotBuf  []byte

	// Security
	caCertPath     string
	nodeCertPath   string
	nodeKeyPath    string
	peerServerName string

	// Options/Fault simulation
	debug             bool
	snapshotThreshold int
	partition         map[string]bool
	networkDelay      time.Duration
}

// NewRaft initializes a new Raft node.
func NewRaft(
	id string,
	peers map[string]string,
	storageDir string,
	caCertPath, nodeCertPath, nodeKeyPath, peerServerName string,
	debug bool,
	snapshotThreshold int,
) (*Raft, error) {
	walPath := fmt.Sprintf("%s/wal.log", storageDir)
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		return nil, err
	}

	r := &Raft{
		id:                id,
		peers:             peers,
		grpcPeers:         make(map[string]*peerClient),
		state:             Follower,
		votesReceived:     make(map[string]bool),
		nextIndex:         make(map[string]int64),
		matchIndex:        make(map[string]int64),
		rpcCh:             make(chan rpcReq, 100),
		cmdCh:             make(chan cmdReq, 100),
		shutdown:          make(chan struct{}),
		electionTimeout:   150 * time.Millisecond,
		storageDir:        storageDir,
		wal:               wal,
		snapshotPath:      fmt.Sprintf("%s/snapshot.json", storageDir),
		kv:                make(map[string]string),
		caCertPath:        caCertPath,
		nodeCertPath:      nodeCertPath,
		nodeKeyPath:       nodeKeyPath,
		peerServerName:    peerServerName,
		debug:             debug,
		snapshotThreshold: snapshotThreshold,
	}

	if err := r.loadState(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to load persistent state: %w", err)
	}

	return r, nil
}

// Start runs the consensus loop and establishes peer connections.
func (r *Raft) Start() error {
	if err := r.connectToPeers(); err != nil {
		return err
	}
	go r.run()
	return nil
}

// Stop shuts down the Raft node.
func (r *Raft) Stop() {
	r.mu.Lock()
	r.state = Shutdown
	close(r.shutdown)
	r.mu.Unlock()

	for _, pc := range r.grpcPeers {
		pc.conn.Close()
	}
	r.wal.Close()
}

// Propose appends a client request to the replicated log.
func (r *Raft) Propose(op, key, val string) (bool, error) {
	cmd := Command{Op: op, Key: key, Val: val}
	data, err := json.Marshal(cmd)
	if err != nil {
		return false, err
	}

	respCh := make(chan cmdResp, 1)
	select {
	case r.cmdCh <- cmdReq{cmd: data, resp: respCh}:
	case <-r.shutdown:
		return false, errors.New("node shutdown")
	}

	res := <-respCh
	return res.success, res.err
}

// Get reads from the local state machine (leader-only).
func (r *Raft) Get(key string) (string, bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader {
		return "", false, false
	}
	val, ok := r.kv[key]
	return val, ok, true
}

// IsLeader checks if the node is leader and returns the leader ID if known.
func (r *Raft) IsLeader() (bool, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Leader {
		return true, r.id
	}
	// We can try to infer the leader based on who we voted for or AppendEntries.
	return false, r.votedFor
}

// Simulated network partition
func (r *Raft) SetPartition(partition map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partition = partition
}

// Simulated network delay
func (r *Raft) SetNetworkDelay(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.networkDelay = d
}

// --- Internal Consensus Logic ---

func (r *Raft) run() {
	r.mu.Lock()
	r.resetElectionTimer()
	r.mu.Unlock()

	for {
		select {
		case <-r.shutdown:
			return

		case <-r.electionTimer.C:
			r.mu.Lock()
			if r.state == Follower || r.state == Candidate {
				r.transitionToCandidate()
			}
			r.mu.Unlock()

		case rpcMsg := <-r.rpcCh:
			r.mu.Lock()
			r.handleRPC(rpcMsg)
			r.mu.Unlock()

		case cmdMsg := <-r.cmdCh:
			r.mu.Lock()
			r.handleClientCommand(cmdMsg)
			r.mu.Unlock()
		}
	}
}

func (r *Raft) resetElectionTimer() {
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	d := r.electionTimeout + time.Duration(rand.Intn(150))*time.Millisecond
	r.electionTimer = time.NewTimer(d)
}

func (r *Raft) transitionToFollower(term int64) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.resetElectionTimer()
	r.logInfo("Transitioned to Follower for term %d", term)

	// Cancel current proposals
	for _, p := range r.proposals {
		p.respCh <- cmdResp{success: false, err: errors.New("node transitioned to follower")}
	}
	r.proposals = nil
}

func (r *Raft) transitionToCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.resetElectionTimer()
	r.votesReceived = make(map[string]bool)
	r.votesReceived[r.id] = true

	r.logInfo("Transitioned to Candidate for term %d, starting election", r.currentTerm)

	if err := r.wal.AppendState(r.currentTerm, r.votedFor); err != nil {
		r.logError("Failed to persist state on candidate transition: %v", err)
	}

	r.startElection()
}

func (r *Raft) transitionToLeader() {
	if r.state != Candidate {
		return
	}
	r.state = Leader
	r.logInfo("Transitioned to Leader for term %d", r.currentTerm)

	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}

	lastIdx := r.lastLogIndex()
	for peerID := range r.peers {
		r.nextIndex[peerID] = lastIdx + 1
		r.matchIndex[peerID] = 0
	}

	r.broadcastHeartbeats()
	r.heartbeatTicker = time.NewTicker(50 * time.Millisecond)

	go func() {
		for {
			select {
			case <-r.shutdown:
				return
			case <-r.heartbeatTicker.C:
				r.mu.Lock()
				if r.state == Leader {
					r.broadcastHeartbeats()
				} else {
					r.heartbeatTicker.Stop()
					r.mu.Unlock()
					return
				}
				r.mu.Unlock()
			}
		}
	}()
}

func (r *Raft) startElection() {
	term := r.currentTerm
	lastLogIdx := r.lastLogIndex()
	lastLogTerm := r.lastLogTerm()

	if len(r.peers) == 0 {
		r.transitionToLeader()
		return
	}

	for peerID, peerAddr := range r.peers {
		go func(pid, addr string) {
			resp, err := r.sendRequestVote(pid, term, lastLogIdx, lastLogTerm)
			if err != nil {
				r.logDebug("Failed to send RequestVote to %s: %v", pid, err)
				return
			}
			r.rpcCh <- rpcReq{
				req: &voteRespMsg{
					peerID: pid,
					resp:   resp,
				},
			}
		}(peerID, peerAddr)
	}
}

func (r *Raft) broadcastHeartbeats() {
	for peerID := range r.peers {
		go r.replicateToPeer(peerID)
	}
}

func (r *Raft) replicateToPeer(peerID string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	nextIdx := r.nextIndex[peerID]
	lastIdx := r.lastLogIndex()
	term := r.currentTerm
	leaderCommit := r.commitIndex

	if nextIdx <= r.lastIncludedIndex {
		go r.sendSnapshotToPeer(peerID)
		r.mu.Unlock()
		return
	}

	prevLogIdx := nextIdx - 1
	prevLogTerm := r.getEntryTerm(prevLogIdx)

	var entries []*raftproto.LogEntry
	if lastIdx >= nextIdx {
		for i := nextIdx; i <= lastIdx; i++ {
			entries = append(entries, r.getEntry(i))
		}
	}
	r.mu.Unlock()

	resp, err := r.sendAppendEntries(peerID, term, prevLogIdx, prevLogTerm, entries, leaderCommit)
	if err != nil {
		r.logDebug("Failed to send AppendEntries to %s: %v", peerID, err)
		return
	}

	r.rpcCh <- rpcReq{
		req: &appendRespMsg{
			peerID: peerID,
			resp:   resp,
		},
	}
}

func (r *Raft) sendSnapshotToPeer(peerID string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	lastIncludedIndex := r.lastIncludedIndex
	lastIncludedTerm := r.lastIncludedTerm

	snap, err := storage.LoadSnapshot(r.snapshotPath)
	if err != nil {
		r.logError("Failed to load snapshot for replication to %s: %v", peerID, err)
		r.mu.Unlock()
		return
	}
	if snap == nil {
		r.mu.Unlock()
		return
	}

	snapData, err := json.Marshal(snap)
	if err != nil {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	chunkSize := 64 * 1024
	offset := int64(0)
	for {
		end := int(offset) + chunkSize
		done := false
		if end >= len(snapData) {
			end = len(snapData)
			done = true
		}

		chunk := snapData[offset:end]
		resp, err := r.sendInstallSnapshot(peerID, term, lastIncludedIndex, lastIncludedTerm, offset, chunk, done)
		if err != nil {
			r.logDebug("Failed to send snapshot chunk to %s: %v", peerID, err)
			return
		}

		if resp.Term > term {
			r.rpcCh <- rpcReq{
				req: &snapshotRespMsg{
					peerID: peerID,
					resp:   resp,
				},
			}
			return
		}

		if done {
			r.rpcCh <- rpcReq{
				req: &snapshotRespMsg{
					peerID: peerID,
					resp:   resp,
				},
			}
			break
		}
		offset = int64(end)
	}
}

func (r *Raft) handleRPC(msg rpcReq) {
	switch m := msg.req.(type) {
	case *raftproto.RequestVoteRequest:
		resp := r.processRequestVote(m)
		if msg.resp != nil {
			msg.resp <- resp
		}
	case *raftproto.AppendEntriesRequest:
		resp := r.processAppendEntries(m)
		if msg.resp != nil {
			msg.resp <- resp
		}
	case *raftproto.InstallSnapshotRequest:
		resp := r.processInstallSnapshot(m)
		if msg.resp != nil {
			msg.resp <- resp
		}
	case *voteRespMsg:
		r.processRequestVoteResponse(m)
	case *appendRespMsg:
		r.processAppendEntriesResponse(m)
	case *snapshotRespMsg:
		r.processInstallSnapshotResponse(m)
	}
}

func (r *Raft) processRequestVote(req *raftproto.RequestVoteRequest) *raftproto.RequestVoteResponse {
	if req.Term < r.currentTerm {
		return &raftproto.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	if req.Term > r.currentTerm {
		r.transitionToFollower(req.Term)
	}

	lastTerm := r.lastLogTerm()
	lastIdx := r.lastLogIndex()
	logOK := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

	if (r.votedFor == "" || r.votedFor == req.CandidateId) && logOK {
		r.votedFor = req.CandidateId
		r.resetElectionTimer()
		if err := r.wal.AppendState(r.currentTerm, r.votedFor); err != nil {
			r.logError("Failed to save state on RequestVote: %v", err)
		}
		return &raftproto.RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}
	}

	return &raftproto.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
}

func (r *Raft) processRequestVoteResponse(msg *voteRespMsg) {
	if msg.resp.Term > r.currentTerm {
		r.transitionToFollower(msg.resp.Term)
		return
	}

	if r.state == Candidate && msg.resp.Term == r.currentTerm && msg.resp.VoteGranted {
		r.votesReceived[msg.peerID] = true
		majority := (len(r.peers)+1)/2 + 1
		if len(r.votesReceived) >= majority {
			r.transitionToLeader()
		}
	}
}

func (r *Raft) processAppendEntries(req *raftproto.AppendEntriesRequest) *raftproto.AppendEntriesResponse {
	if req.Term < r.currentTerm {
		return &raftproto.AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}

	if req.Term > r.currentTerm {
		r.transitionToFollower(req.Term)
	}

	if r.state == Candidate {
		r.transitionToFollower(req.Term)
	}

	// Identify leader to point client to
	r.votedFor = req.LeaderId
	r.resetElectionTimer()

	// Check prev log match
	if req.PrevLogIndex > r.lastLogIndex() {
		return &raftproto.AppendEntriesResponse{Term: r.currentTerm, Success: false, MatchIndex: r.lastLogIndex()}
	}

	if req.PrevLogIndex >= r.lastIncludedIndex {
		prevTerm := r.getEntryTerm(req.PrevLogIndex)
		if prevTerm != req.PrevLogTerm {
			return &raftproto.AppendEntriesResponse{Term: r.currentTerm, Success: false, MatchIndex: req.PrevLogIndex - 1}
		}
	}

	// Process appends
	for _, entry := range req.Entries {
		if entry.Index <= r.lastIncludedIndex {
			continue
		}

		if entry.Index <= r.lastLogIndex() {
			existing := r.getEntry(entry.Index)
			if existing != nil && existing.Term != entry.Term {
				// Conflict: truncate and write WAL marker
				r.log = r.log[:entry.Index-r.lastIncludedIndex-1]
				if err := r.wal.AppendTruncate(entry.Index); err != nil {
					r.logError("Failed to write truncate marker to WAL: %v", err)
				}
				r.log = append(r.log, entry)
				if err := r.wal.AppendEntry(entry); err != nil {
					r.logError("Failed to write entry to WAL: %v", err)
				}
			}
		} else {
			// Append new entry
			r.log = append(r.log, entry)
			if err := r.wal.AppendEntry(entry); err != nil {
				r.logError("Failed to write entry to WAL: %v", err)
			}
		}
	}

	// Commit entries
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = req.LeaderCommit
		if r.commitIndex > r.lastLogIndex() {
			r.commitIndex = r.lastLogIndex()
		}
		r.applyLogs()
	}

	return &raftproto.AppendEntriesResponse{
		Term:       r.currentTerm,
		Success:    true,
		MatchIndex: r.lastLogIndex(),
	}
}

func (r *Raft) processAppendEntriesResponse(msg *appendRespMsg) {
	if msg.resp.Term > r.currentTerm {
		r.transitionToFollower(msg.resp.Term)
		return
	}

	if r.state == Leader && msg.resp.Term == r.currentTerm {
		if msg.resp.Success {
			r.matchIndex[msg.peerID] = msg.resp.MatchIndex
			r.nextIndex[msg.peerID] = msg.resp.MatchIndex + 1

			// Check for commit progression
			majority := (len(r.peers)+1)/2 + 1
			lastIdx := r.lastLogIndex()
			for n := r.commitIndex + 1; n <= lastIdx; n++ {
				if r.getEntryTerm(n) == r.currentTerm {
					count := 1 // self
					for _, mIdx := range r.matchIndex {
						if mIdx >= n {
							count++
						}
					}
					if count >= majority {
						r.commitIndex = n
						r.applyLogs()
					}
				}
			}
		} else {
			// Fast roll-back nextIndex
			r.nextIndex[msg.peerID] = msg.resp.MatchIndex + 1
			if r.nextIndex[msg.peerID] < 1 {
				r.nextIndex[msg.peerID] = 1
			}
			go r.replicateToPeer(msg.peerID)
		}
	}
}

func (r *Raft) processInstallSnapshot(req *raftproto.InstallSnapshotRequest) *raftproto.InstallSnapshotResponse {
	if req.Term < r.currentTerm {
		return &raftproto.InstallSnapshotResponse{Term: r.currentTerm}
	}

	if req.Term > r.currentTerm {
		r.transitionToFollower(req.Term)
	}

	if r.state == Candidate {
		r.transitionToFollower(req.Term)
	}

	r.votedFor = req.LeaderId
	r.resetElectionTimer()

	if req.Offset == 0 {
		r.snapshotBuf = nil
	}

	if req.Offset == int64(len(r.snapshotBuf)) {
		r.snapshotBuf = append(r.snapshotBuf, req.Data...)
	}

	if req.Done {
		var snap storage.Snapshot
		if err := json.Unmarshal(r.snapshotBuf, &snap); err == nil {
			r.restoreSnapshot(&snap)
		} else {
			r.logError("Failed to unmarshal received snapshot: %v", err)
		}
		r.snapshotBuf = nil
	}

	return &raftproto.InstallSnapshotResponse{Term: r.currentTerm}
}

func (r *Raft) processInstallSnapshotResponse(msg *snapshotRespMsg) {
	if msg.resp.Term > r.currentTerm {
		r.transitionToFollower(msg.resp.Term)
		return
	}

	if r.state == Leader && msg.resp.Term == r.currentTerm {
		r.matchIndex[msg.peerID] = r.lastIncludedIndex
		r.nextIndex[msg.peerID] = r.lastIncludedIndex + 1
		go r.replicateToPeer(msg.peerID)
	}
}

func (r *Raft) handleClientCommand(msg cmdReq) {
	if r.state != Leader {
		msg.resp <- cmdResp{success: false, err: errors.New("not the leader")}
		return
	}

	entry := &raftproto.LogEntry{
		Index:   r.lastLogIndex() + 1,
		Term:    r.currentTerm,
		Command: msg.cmd,
	}

	r.log = append(r.log, entry)
	if err := r.wal.AppendEntry(entry); err != nil {
		r.logError("Failed to save client entry: %v", err)
		msg.resp <- cmdResp{success: false, err: err}
		return
	}

	p := &proposal{
		index:  entry.Index,
		term:   entry.Term,
		respCh: msg.resp,
	}
	r.proposals = append(r.proposals, p)

	r.broadcastHeartbeats()
}

func (r *Raft) applyLogs() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry := r.getEntry(r.lastApplied)
		if entry != nil && len(entry.Command) > 0 {
			r.applyCommand(entry.Command)
		}
	}

	// Trigger compaction
	if r.snapshotThreshold > 0 && int(r.lastLogIndex()-r.lastIncludedIndex) > r.snapshotThreshold {
		r.takeSnapshot()
	}

	// Complete proposals
	var remaining []*proposal
	for _, p := range r.proposals {
		if p.index <= r.commitIndex {
			if p.term == r.getEntryTerm(p.index) {
				p.respCh <- cmdResp{success: true, err: nil}
			} else {
				p.respCh <- cmdResp{success: false, err: errors.New("leader stepped down before committing")}
			}
		} else {
			remaining = append(remaining, p)
		}
	}
	r.proposals = remaining
}

func (r *Raft) applyCommand(cmdBytes []byte) {
	var cmd Command
	if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
		r.logError("Failed to unmarshal command for application: %v", err)
		return
	}

	switch cmd.Op {
	case "PUT":
		r.kv[cmd.Key] = cmd.Val
		r.logInfo("Applied PUT for key: %s (size: %d bytes)", cmd.Key, len(cmd.Val))
	case "DEL":
		delete(r.kv, cmd.Key)
		r.logInfo("Applied DEL for key: %s", cmd.Key)
	}
}

func (r *Raft) takeSnapshot() {
	r.logInfo("Compacting logs, taking snapshot up to index %d...", r.commitIndex)

	kvCopy := make(map[string]string)
	for k, v := range r.kv {
		kvCopy[k] = v
	}

	snap := &storage.Snapshot{
		LastIncludedIndex: r.commitIndex,
		LastIncludedTerm:  r.getEntryTerm(r.commitIndex),
		KVState:           kvCopy,
	}

	if err := storage.SaveSnapshot(r.snapshotPath, snap); err != nil {
		r.logError("Failed to save snapshot: %v", err)
		return
	}

	var newLog []*raftproto.LogEntry
	for i := r.commitIndex + 1; i <= r.lastLogIndex(); i++ {
		newLog = append(newLog, r.getEntry(i))
	}

	r.lastIncludedIndex = snap.LastIncludedIndex
	r.lastIncludedTerm = snap.LastIncludedTerm
	r.log = newLog

	if err := r.wal.Reset(); err != nil {
		r.logError("Failed to reset WAL: %v", err)
		return
	}
	if err := r.wal.AppendState(r.currentTerm, r.votedFor); err != nil {
		r.logError("Failed to append state to WAL after snapshot: %v", err)
	}
}

func (r *Raft) restoreSnapshot(snap *storage.Snapshot) {
	r.logInfo("Restoring snapshot up to index %d...", snap.LastIncludedIndex)

	if err := storage.SaveSnapshot(r.snapshotPath, snap); err != nil {
		r.logError("Failed to save restored snapshot to disk: %v", err)
		return
	}

	r.kv = make(map[string]string)
	for k, v := range snap.KVState {
		r.kv[k] = v
	}

	var newLog []*raftproto.LogEntry
	for i := snap.LastIncludedIndex + 1; i <= r.lastLogIndex(); i++ {
		e := r.getEntry(i)
		if e != nil {
			newLog = append(newLog, e)
		}
	}

	r.lastIncludedIndex = snap.LastIncludedIndex
	r.lastIncludedTerm = snap.LastIncludedTerm
	r.log = newLog

	if r.commitIndex < snap.LastIncludedIndex {
		r.commitIndex = snap.LastIncludedIndex
	}
	if r.lastApplied < snap.LastIncludedIndex {
		r.lastApplied = snap.LastIncludedIndex
	}

	if err := r.wal.Reset(); err != nil {
		r.logError("Failed to reset WAL: %v", err)
	}
	if err := r.wal.AppendState(r.currentTerm, r.votedFor); err != nil {
		r.logError("Failed to append state to WAL after restore: %v", err)
	}
}

func (r *Raft) loadState() error {
	snap, err := storage.LoadSnapshot(r.snapshotPath)
	if err != nil {
		return err
	}

	if snap != nil {
		r.kv = snap.KVState
		r.lastIncludedIndex = snap.LastIncludedIndex
		r.lastIncludedTerm = snap.LastIncludedTerm
		r.commitIndex = snap.LastIncludedIndex
		r.lastApplied = snap.LastIncludedIndex
		r.logInfo("Loaded snapshot up to index %d, term %d", r.lastIncludedIndex, r.lastIncludedTerm)
	} else {
		r.kv = make(map[string]string)
	}

	term, votedFor, entries, err := r.wal.Replay()
	if err != nil {
		return err
	}

	if term > 0 {
		r.currentTerm = term
		r.votedFor = votedFor
	}

	for _, e := range entries {
		if e.Index > r.lastIncludedIndex {
			r.log = append(r.log, e)
		}
	}

	r.logInfo("Loaded WAL entries. In-memory log entries count: %d, Term: %d, VotedFor: %s", len(r.log), r.currentTerm, r.votedFor)
	r.applyLogs()
	return nil
}

// --- Helpers and Index Translation ---

func (r *Raft) lastLogIndex() int64 {
	if len(r.log) == 0 {
		return r.lastIncludedIndex
	}
	return r.log[len(r.log)-1].Index
}

func (r *Raft) lastLogTerm() int64 {
	if len(r.log) == 0 {
		return r.lastIncludedTerm
	}
	return r.log[len(r.log)-1].Term
}

func (r *Raft) getEntry(idx int64) *raftproto.LogEntry {
	if idx == r.lastIncludedIndex {
		return &raftproto.LogEntry{
			Index: r.lastIncludedIndex,
			Term:  r.lastIncludedTerm,
		}
	}
	if idx < r.lastIncludedIndex {
		return nil
	}
	offset := idx - r.lastIncludedIndex - 1
	if offset >= 0 && offset < int64(len(r.log)) {
		return r.log[offset]
	}
	return nil
}

func (r *Raft) getEntryTerm(idx int64) int64 {
	entry := r.getEntry(idx)
	if entry != nil {
		return entry.Term
	}
	return 0
}

// --- Network Calls with Fault Injection ---

func (r *Raft) checkNetworkFault(peerID string) error {
	r.mu.Lock()
	delay := r.networkDelay
	isPartitioned := r.partition != nil && !r.partition[peerID]
	r.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if isPartitioned {
		return status.Errorf(codes.Unavailable, "simulated network partition for peer %s", peerID)
	}
	return nil
}

func (r *Raft) sendRequestVote(peerID string, term, lastLogIdx, lastLogTerm int64) (*raftproto.RequestVoteResponse, error) {
	if err := r.checkNetworkFault(peerID); err != nil {
		return nil, err
	}

	r.mu.Lock()
	pc, ok := r.grpcPeers[peerID]
	r.mu.Unlock()

	if !ok {
		return nil, errors.New("gRPC client connection not found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	return pc.cli.RequestVote(ctx, &raftproto.RequestVoteRequest{
		Term:         term,
		CandidateId:  r.id,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	})
}

func (r *Raft) sendAppendEntries(peerID string, term, prevLogIdx, prevLogTerm int64, entries []*raftproto.LogEntry, leaderCommit int64) (*raftproto.AppendEntriesResponse, error) {
	if err := r.checkNetworkFault(peerID); err != nil {
		return nil, err
	}

	r.mu.Lock()
	pc, ok := r.grpcPeers[peerID]
	r.mu.Unlock()

	if !ok {
		return nil, errors.New("gRPC client connection not found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	return pc.cli.AppendEntries(ctx, &raftproto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
}

func (r *Raft) sendInstallSnapshot(peerID string, term, lastIncludedIndex, lastIncludedTerm int64, offset int64, data []byte, done bool) (*raftproto.InstallSnapshotResponse, error) {
	if err := r.checkNetworkFault(peerID); err != nil {
		return nil, err
	}

	r.mu.Lock()
	pc, ok := r.grpcPeers[peerID]
	r.mu.Unlock()

	if !ok {
		return nil, errors.New("gRPC client connection not found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	return pc.cli.InstallSnapshot(ctx, &raftproto.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          r.id,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            offset,
		Data:              data,
		Done:              done,
	})
}

func (r *Raft) connectToPeers() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for peerID, addr := range r.peers {
		creds, err := transport.LoadClientCredentials(r.caCertPath, r.nodeCertPath, r.nodeKeyPath, r.peerServerName)
		if err != nil {
			return fmt.Errorf("failed to load credentials for client mTLS connecting to %s: %w", peerID, err)
		}

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return fmt.Errorf("failed to dial peer %s at %s: %w", peerID, addr, err)
		}

		r.grpcPeers[peerID] = &peerClient{
			cli:  raftproto.NewRaftServiceClient(conn),
			conn: conn,
		}
	}
	return nil
}

// --- gRPC Server Implementations ---

func (r *Raft) RequestVote(ctx context.Context, req *raftproto.RequestVoteRequest) (*raftproto.RequestVoteResponse, error) {
	respCh := make(chan interface{}, 1)
	select {
	case r.rpcCh <- rpcReq{req: req, resp: respCh}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.shutdown:
		return nil, status.Errorf(codes.Unavailable, "node is shutting down")
	}

	select {
	case res := <-respCh:
		return res.(*raftproto.RequestVoteResponse), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Raft) AppendEntries(ctx context.Context, req *raftproto.AppendEntriesRequest) (*raftproto.AppendEntriesResponse, error) {
	respCh := make(chan interface{}, 1)
	select {
	case r.rpcCh <- rpcReq{req: req, resp: respCh}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.shutdown:
		return nil, status.Errorf(codes.Unavailable, "node is shutting down")
	}

	select {
	case res := <-respCh:
		return res.(*raftproto.AppendEntriesResponse), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Raft) InstallSnapshot(ctx context.Context, req *raftproto.InstallSnapshotRequest) (*raftproto.InstallSnapshotResponse, error) {
	respCh := make(chan interface{}, 1)
	select {
	case r.rpcCh <- rpcReq{req: req, resp: respCh}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.shutdown:
		return nil, status.Errorf(codes.Unavailable, "node is shutting down")
	}

	select {
	case res := <-respCh:
		return res.(*raftproto.InstallSnapshotResponse), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// --- logging helpers ---

func (r *Raft) logInfo(format string, v ...interface{}) {
	logMsg := fmt.Sprintf(format, v...)
	fmt.Printf("[INFO] [Node %s] %s\n", r.id, logMsg)
}

func (r *Raft) logDebug(format string, v ...interface{}) {
	if r.debug {
		logMsg := fmt.Sprintf(format, v...)
		fmt.Printf("[DEBUG] [Node %s] %s\n", r.id, logMsg)
	}
}

func (r *Raft) logError(format string, v ...interface{}) {
	logMsg := fmt.Sprintf(format, v...)
	fmt.Printf("[ERROR] [Node %s] %s\n", r.id, logMsg)
}
