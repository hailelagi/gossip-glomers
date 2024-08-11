package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type session struct {
	node    *maelstrom.Node
	kv      *store
	retries chan retry
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

func (s *session) transactionHandler(msg maelstrom.Message) error {
	var body map[string]any
	var wg sync.WaitGroup
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	for _, dest := range s.node.NodeIDs() {
		wg.Add(1)

		go func(dest string) {
			deadline := time.Now().Add(400 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			replicaBody := map[string]any{"type": "replicate", "txn": body["txn"]}

			defer cancel()
			defer wg.Done()

			_, err := s.node.SyncRPC(ctx, dest, replicaBody)

			if err == nil {
				return
			} else {
				s.retries <- retry{body: replicaBody, dest: dest, attempt: 20, err: err}
			}
		}(dest)
	}

	wg.Wait()

	if len(s.retries) > 0 {
		return s.node.Reply(
			msg,
			map[string]any{
				"type": "error",
				"code": maelstrom.TxnConflict,
				"text": "txn abort",
			},
		)
	}

	body["type"] = "txn_ok"
	return s.node.Reply(msg, body)
}

func (s *session) replicateHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.kv.Commit(body["txn"].([]any))
	return s.node.Reply(msg, map[string]any{"type": "replicate_ok"})
}

func failureDetector(s *session) {
	var atttempts sync.WaitGroup

	for r := range s.retries {
		r := r

		if r.dest == r.body["src"] || r.dest == s.node.ID() {
			continue
		}

		atttempts.Add(1)

		go func(retry retry, attempts *sync.WaitGroup) {
			deadline := time.Now().Add(800 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			defer attempts.Done()

			retry.attempt--

			if retry.attempt >= 0 {
				_, err := s.node.SyncRPC(ctx, retry.dest, retry.body)

				if err == nil {
					return
				} else {
					s.retries <- retry
				}

			} else {
				log.SetOutput(os.Stderr)
				log.Printf("dead letter message slip loss beyond tolerance %v", retry)
			}
		}(r, &atttempts)
	}

	atttempts.Wait()
}

func main() {
	n := maelstrom.NewNode()

	s := &session{
		node:    n,
		retries: make(chan retry, 1000),
		kv: &store{index: map[int]int{},
			log: make([]float64, 1000_000)},
	}

	n.Handle("txn", s.transactionHandler)
	n.Handle("replicate", s.replicateHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
