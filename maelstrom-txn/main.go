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

type store struct {
	index map[int]int
	log   []float64
	sync.RWMutex
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

var neighbors []any

func (s *session) topologyHandler(msg maelstrom.Message) error {
	var body = make(map[string]any)

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	self := s.node.ID()
	topology := body["topology"].(map[string]any)
	neighbors = topology[self].([]any)

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *session) transactionHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	kv := s.kv
	n := s.node

	kv.Lock()
	defer kv.Unlock()

	txn := body["txn"].([]any)
	var result = make([][]any, 0)

	for _, op := range txn {
		op := op.([]any)

		if op[0] == "r" {
			index := op[1].(float64)

			result = append(result, []any{"r", index, kv.log[int(index)]})
		} else if op[0] == "w" {
			index := op[1].(float64)
			value := op[2].(float64)
			kv.log[int(index)] = value

			result = append(result, []any{"w", index, kv.log[int(index)]})
		}
	}

	for _, dest := range neighbors {
		if dest == msg.Src || dest == s.node.ID() {
			continue
		}

		wg.Add(1)

		go func(dest string) {
			deadline := time.Now().Add(400 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			defer wg.Done()

			_, err := n.SyncRPC(ctx, dest, body)

			if err == nil {
				return
			} else {
				s.retries <- retry{body: body, dest: dest, attempt: 20, err: err}
			}
		}(dest.(string))
	}

	wg.Wait()

	body["txn"] = result
	body["type"] = "txn_ok"
	return s.node.Reply(msg, body)
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
		node: n,
		kv:   &store{index: map[int]int{}, log: make([]float64, 1000_000)},
	}

	n.Handle("topology", s.topologyHandler)
	n.Handle("txn", s.transactionHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
