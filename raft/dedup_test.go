package raft

import (
	"testing"
	"time"
)

func TestClientRequestDeduplication(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.stop()

	// Wait for leader election
	var leader *Raft
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		for _, node := range tc.nodes {
			if isLeader, _ := node.IsLeader(); isLeader {
				leader = node
				break
			}
		}
		if leader != nil {
			break
		}
	}

	if leader == nil {
		t.Fatalf("cluster failed to elect a leader within 5 seconds")
	}

	clientID := "client-uuid-1234"
	reqID := int64(1)

	// First proposal with clientID and reqID
	ok, err := leader.ProposeWithClient("PUT", "dedup_key", "value_v1", clientID, reqID)
	if err != nil || !ok {
		t.Fatalf("first ProposeWithClient failed: %v", err)
	}

	// Verify key value in state machine
	val, exists, isLeader := leader.Get("dedup_key")
	if !isLeader || !exists || val != "value_v1" {
		t.Fatalf("expected key 'dedup_key' to have value 'value_v1', got val=%s exists=%v isLeader=%v", val, exists, isLeader)
	}

	// Submit duplicate proposal with SAME clientID and reqID, but DIFFERENT value payload
	ok2, err2 := leader.ProposeWithClient("PUT", "dedup_key", "value_v2_duplicate", clientID, reqID)
	if err2 != nil || !ok2 {
		t.Fatalf("duplicate ProposeWithClient failed: %v", err2)
	}

	// Verify that state machine WAS NOT mutated by the duplicate request (remains 'value_v1')
	valAfterDup, existsAfterDup, _ := leader.Get("dedup_key")
	if !existsAfterDup || valAfterDup != "value_v1" {
		t.Fatalf("deduplication failed! expected state machine to remain 'value_v1', but got '%s'", valAfterDup)
	}
}
