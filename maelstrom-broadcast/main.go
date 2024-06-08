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
func (s *session) topologyHandler(msg maelstrom.Message) error {
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
				s.retries <- retry{body: body, dest: dest, attempt: 10, err: err}
			}
		}(ctx, s, dest, body, &wg)
	}

	wg.Wait()

	response := map[string]any{"type": "broadcast_ok", "msg_id": body["msg_id"]}
	return s.node.Reply(msg, response)
}

func failureDetector(s *session, retries chan retry) {
	for r := range s.retries {
		r := r

		go func(retry retry) {
			deadline := time.Now().Add(300 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			retry.attempt--

			if retry.attempt >= 0 {
				_, err := s.node.SyncRPC(ctx, retry.dest, retry.body)
				if err != nil {
					retries <- retry
				}
			} else {
				log.SetOutput(os.Stderr)
				log.Printf("message slip loss beyond tolerance from queue %v", retry)
			}
		}(r)
	}
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
	var retries = make(chan retry, 2400)

	s := &session{
		node: n, retries: retries,
		store: &store{index: map[float64]bool{}, log: []float64{}},
	}

	n.Handle("topology", s.topologyHandler)
	n.Handle("read", s.readHandler)
	n.Handle("broadcast", s.broadcastHandler)

	// background failure detectors
	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s, retries)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
