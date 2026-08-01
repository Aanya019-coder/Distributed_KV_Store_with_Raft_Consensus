package raft

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

type KvInput struct {
	Op        string // "GET", "PUT", "DEL"
	Key       string
	Value     string
	ClientID  string
	RequestID int64
}

type KvOutput struct {
	Value string
	Ok    bool
}

var kvModel = porcupine.Model{
	Init: func() interface{} {
		return make(map[string]string)
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(map[string]string)
		inp := input.(KvInput)
		out := output.(KvOutput)

		nextState := make(map[string]string)
		for k, v := range st {
			nextState[k] = v
		}

		switch inp.Op {
		case "GET":
			if out.Ok {
				expectedVal := st[inp.Key]
				if out.Value == expectedVal {
					return true, nextState
				}
				return false, nextState
			}
			return true, nextState

		case "PUT":
			if out.Ok {
				nextState[inp.Key] = inp.Value
				return true, nextState
			}
			return true, nextState

		case "DEL":
			if out.Ok {
				delete(nextState, inp.Key)
				return true, nextState
			}
			return true, nextState
		}

		return false, nextState
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(KvInput)
		out := output.(KvOutput)
		if inp.Op == "GET" {
			return fmt.Sprintf("GET(%s) -> %s (ok=%v)", inp.Key, out.Value, out.Ok)
		} else if inp.Op == "PUT" {
			return fmt.Sprintf("PUT(%s, %s) -> ok=%v", inp.Key, inp.Value, out.Ok)
		} else {
			return fmt.Sprintf("DEL(%s) -> ok=%v", inp.Key, out.Ok)
		}
	},
}

func TestLinearizability(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.stop()

	// Wait for leader
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
		t.Fatalf("cluster failed to elect initial leader")
	}

	opsLock := sync.Mutex{}
	var operations []porcupine.Operation

	numClients := 4
	reqsPerClient := 8
	var wg sync.WaitGroup

	startTime := time.Now()

	// Launch concurrent clients
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		clientID := fmt.Sprintf("client-%d", c)
		go func(cid string, clientIdx int) {
			defer wg.Done()

			keys := []string{"keyA", "keyB"}

			for reqID := int64(1); reqID <= int64(reqsPerClient); reqID++ {
				key := keys[rand.Intn(len(keys))]
				opType := "PUT"
				if reqID%3 == 0 {
					opType = "GET"
				} else if reqID%5 == 0 {
					opType = "DEL"
				}
				val := fmt.Sprintf("val-%s-%d", cid, reqID)

				input := KvInput{
					Op:        opType,
					Key:       key,
					Value:     val,
					ClientID:  cid,
					RequestID: reqID,
				}

				callTime := time.Since(startTime).Nanoseconds()

				// Execute against current cluster leader
				var outVal string
				var ok bool

				// Retry finding active leader
				for attempt := 0; attempt < 5; attempt++ {
					var curLeader *Raft
					for _, node := range tc.nodes {
						if isL, _ := node.IsLeader(); isL {
							curLeader = node
							break
						}
					}

					if curLeader != nil {
						if opType == "PUT" {
							ok, _ = curLeader.ProposeWithClient("PUT", key, val, cid, reqID)
						} else if opType == "DEL" {
							ok, _ = curLeader.ProposeWithClient("DEL", key, "", cid, reqID)
						} else {
							outVal, ok, _ = curLeader.Get(key)
						}
						if ok {
							break
						}
					}
					time.Sleep(50 * time.Millisecond)
				}

				returnTime := time.Since(startTime).Nanoseconds()

				output := KvOutput{
					Value: outVal,
					Ok:    ok,
				}

				op := porcupine.Operation{
					ClientId: clientIdx,
					Input:    input,
					Call:     callTime,
					Output:   output,
					Return:   returnTime,
				}

				opsLock.Lock()
				operations = append(operations, op)
				opsLock.Unlock()

				time.Sleep(20 * time.Millisecond)
			}
		}(clientID, c)
	}

	wg.Wait()

	t.Logf("Recorded %d operations for Porcupine linearizability check", len(operations))

	// Verify linearizability
	res, info := porcupine.CheckOperationsVerbose(kvModel, operations, 10*time.Second)

	// Visualize history to HTML
	vizFile := "porcupine_visualization.html"
	if f, err := os.Create(vizFile); err == nil {
		porcupine.Visualize(kvModel, info, f)
		f.Close()
		t.Logf("Saved Porcupine linearizability visualization to %s", vizFile)
	}

	if res != porcupine.Ok {
		t.Fatalf("Porcupine linearizability check failed! Result: %v, info: %+v", res, info)
	}
}
