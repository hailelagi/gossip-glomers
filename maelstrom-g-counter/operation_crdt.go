package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
// Operation based transform
1: payload Payload type; instantiated at all replicas
2: initial Initial value
3: query Source-local operation (arguments) : returns
4: pre Precondition
5: let Execute at source, synchronously, no side effects
6: update Global update (arguments) : returns
7: atSource (arguments) : returns
8: pre Precondition at source
9: let 1st phase: synchronous, at source, no side effects
10: downstream (arguments passed downstream)
11: pre Precondition against downstream state
12: 2nd phase, asynchronous, side-effects to downstream state
*/

var localCount int

func (s *session) addOperationHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var body map[string]any

	n := s.node

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))

	for _, dest := range n.NodeIDs() {
		wg.Add(1)

		go func(dest string) {
			deadline := time.Now().Add(400 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			replicaBody := map[string]any{"type": "replicate", "delta": delta}

			defer cancel()
			defer wg.Done()

			_, err := n.SyncRPC(ctx, dest, replicaBody)

			if err == nil {
				return
			} else {
				s.retries <- retry{body: replicaBody, dest: dest, attempt: 20, err: err}
			}
		}(dest)
	}

	wg.Wait()

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *session) replicateHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))
	localCount = localCount + delta

	return s.node.Reply(msg, map[string]any{"type": "replicate_ok"})
}

func (s *session) readOperationHandler(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": "read_ok", "value": localCount})
}
