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
	index map[float64]bool
	log   []float64
	sync.RWMutex
}

type session struct {
	node    *maelstrom.Node
	store   *store
	retries chan retry
	mu      sync.Mutex
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

func (s *session) topologyHandler(msg maelstrom.Message) error {
	// TODO: diy yourself a topology of known 'logical' nodes that are discoverable
	var body = make(map[string]any)
	body["type"] = "topology_ok"

	return s.node.Reply(msg, body)

}

func (s *session) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.store.RLock()
	defer s.store.RUnlock()

	body["type"] = "read_ok"
	body["messages"] = s.store.log

	return s.node.Reply(msg, body)
}

func (s *session) broadcastHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["message"].(float64)
	exists := s.store.findOrInsert(key)

	if exists {
		return nil
	}

	for _, dest := range s.node.NodeIDs() {
		wg.Add(1)

		deadline := time.Now().Add(200 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		go func(ctx context.Context, s *session, dest string, body map[string]any, wg *sync.WaitGroup) {
			defer wg.Done()
			_, err := s.node.SyncRPC(ctx, dest, body)

			if err == nil {
				return
			} else {
				s.mu.Lock()
				defer s.mu.Unlock()

				s.retries <- retry{body: body, dest: dest, attempt: 20, err: err}
			}
		}(ctx, s, dest, body, &wg)
	}

	wg.Wait()

	response := map[string]any{"type": "broadcast_ok", "msg_id": body["msg_id"]}
	return s.node.Reply(msg, response)
}

func failureDetector(s *session) {
	var attempts sync.WaitGroup

	for r := range s.retries {
		r := r
		attempts.Add(1)

		go func(retry retry, attempts *sync.WaitGroup) {
			deadline := time.Now().Add(200 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			defer attempts.Done()

			retry.attempt--

			if retry.attempt >= 0 {
				_, err := s.node.SyncRPC(ctx, retry.dest, retry.body)

				if err != nil {
					s.mu.Lock()
					defer s.mu.Unlock()

					s.retries <- retry
				}
			} else {
				log.SetOutput(os.Stderr)
				log.Printf("message slip loss beyond tolerance from queue %v", retry)
			}
		}(r, &attempts)
	}

	attempts.Wait()
}

func (s *store) findOrInsert(key float64) bool {
	s.Lock()
	defer s.Unlock()

	_, exists := s.index[key]

	if !exists {
		s.index[key] = true
		s.log = append(s.log, key)
	}

	return exists
}

func main() {
	n := maelstrom.NewNode()
	/*
	  little's law: L (num units) = arrival rate * wait time (guesstimate)
	  rate == 100 msgs/sec assuming efficient workload, latency/wait mininum = 100ms, 400ms average
	  100 * 0.1 = 10 msgs per request * 25 - 1(self) nodes = 240 queue size
	*/
	var retries = make(chan retry, 240)

	s := &session{
		node: n, retries: retries,
		store: &store{index: map[float64]bool{}, log: []float64{}},
	}

	n.Handle("topology", s.topologyHandler)
	n.Handle("read", s.readHandler)
	n.Handle("broadcast", s.broadcastHandler)

	// background failure detectors
	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
