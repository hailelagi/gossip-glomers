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

type store struct {
	index map[float64]float64
	log   []float64
	sync.RWMutex
}

type session struct {
	node    *maelstrom.Node
	store   *store
	retries chan retry
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

/*
The neighbors Maelstrom suggests are, by default, arranged in a two-dimensional grid.
This means that messages are often duplicated en route to other nodes, and latencies
are on the order of 2 * sqrt(n) network delays.
*/
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

func (s *session) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.store.RLock()
	defer s.store.RUnlock()

	return s.node.Reply(msg, map[string]any{"type": "read_ok", "messages": s.store.log})
}

func (s *session) broadcastHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var body map[string]any
	var store = s.store

	n := s.node

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["message"].(float64)
	exists := store.findOrInsert(key)

	if exists {
		return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	}

	for _, dest := range neighbors {
		wg.Add(1)

		if dest == msg.Src || dest == s.node.ID() {
			continue
		}

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

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func failureDetector(s *session) {
	var atttempts sync.WaitGroup

	for r := range s.retries {
		r := r

		if r.dest == s.node.ID() {
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
				}
				s.retries <- retry

			} else {
				log.SetOutput(os.Stderr)
				log.Printf("dead letter message slip loss beyond tolerance %v", retry)
			}
		}(r, &atttempts)
	}

	atttempts.Wait()
}

func (s *store) findOrInsert(key float64) bool {
	s.Lock()
	defer s.Unlock()

	_, exists := s.index[key]

	if !exists {
		s.index[key] = key
		s.log = append(s.log, key)
	}

	return exists
}

func main() {
	n := maelstrom.NewNode()
	/*
	  little's law: L (num units) = arrival rate * wait time (guesstimate)
	  rate == 100 msgs/sec assuming efficient workload, latency/wait mininum = 100ms, 400ms average
	  100 * 0.4 = 40 msgs per request * 25 - 1(self) nodes = 960 queue size, will use ~1000
	*/
	var retries = make(chan retry, 1000)

	s := &session{
		node: n, retries: retries,
		store: &store{index: map[float64]float64{}, log: []float64{}},
	}

	n.Handle("topology", s.topologyHandler)
	n.Handle("read", s.readHandler)
	n.Handle("broadcast", s.broadcastHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
