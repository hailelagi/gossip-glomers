package main

/*

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var gCounter int64

// the operation is addition and is commutative
// as it is a counter that only ever grows
func (s *session) addOperationHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int64(body["delta"].(float64))
	atomic.AddInt64(&gCounter, delta)

	//replicate count... it is assumed each messsage is idempotent
	for _, dest := range s.node.NodeIDs() {
		wg.Add(1)

		go func(dest string) {
			deadline := time.Now().Add(400 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			defer wg.Done()

			_, err := s.node.SyncRPC(ctx, dest, body)

			if err == nil {
				return
			} else {
				s.retries <- retry{body: body, dest: dest, attempt: 20, err: err}
			}
		}(dest)
	}

	wg.Wait()

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *session) readOperationHandler(msg maelstrom.Message) error {
	var body = map[string]any{"type": "read_ok"}

	delta := atomic.LoadInt64(&gCounter)
	body["value"] = delta

	return s.node.Reply(msg, body)
}

func run() {
	n := maelstrom.NewNode()
	s := &session{node: n, retries: make(chan retry, 100)}

	n.Handle("add", s.addOperationHandler)
	n.Handle("read", s.readOperationHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
*/
