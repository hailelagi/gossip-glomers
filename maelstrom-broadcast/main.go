package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Store struct {
	index map[float64]bool
	log   []float64
	sync.RWMutex
}

type Session struct {
	node    *maelstrom.Node
	store   *Store
	retries chan Retry
}

type Retry struct {
	dest    string
	body    map[string]any
	attempt int
	exec    func(string, any) error
	err     error
}

func (s *Session) topologyHandler(msg maelstrom.Message) error {
	// TODO: diy yourself a topology of known 'logical' nodes that are discoverable
	var body = make(map[string]any)
	body["type"] = "topology_ok"

	return s.node.Reply(msg, body)

}

func (s *Session) readHandler(msg maelstrom.Message) error {
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

func (s *Session) broadcastHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var body map[string]any
	var store = s.store
	n := s.node

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["message"].(float64)
	resp := map[string]any{"type": "broadcast_ok", "msg_id": body["msg_id"]}

	store.Lock()
	_, exists := store.index[key]

	if !exists {
		store.index[key] = true
		store.log = append(store.log, key)
	}
	store.Unlock()

	if exists {
		return s.node.Reply(msg, resp)
	}

	for _, dest := range n.NodeIDs() {
		wg.Add(1)

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
		defer cancel()

		go func(dest string) {
			defer wg.Done()
			_, err := n.SyncRPC(ctx, dest, body)

			if err == nil {
				return
			} else {
				s.retries <- Retry{body: body, dest: dest, attempt: 15, exec: n.Send, err: err}
			}
		}(dest)
	}

	wg.Wait()

	return s.node.Reply(msg, resp)
}

func failureDetector(n *maelstrom.Node, retries chan Retry) {
	for retry := range retries {
		go func(retry Retry) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
			defer cancel()

			retry.attempt--

			if retry.attempt >= 0 {
				_, err := n.SyncRPC(ctx, retry.dest, retry.body)

				if err != nil {
					jitter := time.Duration(rand.Intn(100) + 100)
					time.Sleep(jitter * time.Millisecond)
					retries <- retry
				}
			} else {
				log.SetOutput(os.Stderr)
				log.Printf("message slip loss beyond tolerance from queue %v", retry)
			}
		}(retry)
	}
}

func main() {
	n := maelstrom.NewNode()
	// number of nodes/req * attempts
	retries := make(chan Retry, 250)
	s := &Session{node: n, store: &Store{index: map[float64]bool{}, log: []float64{}}, retries: retries}

	n.Handle("topology", s.topologyHandler)
	n.Handle("read", s.readHandler)
	n.Handle("broadcast", s.broadcastHandler)

	// background failure detection
	go failureDetector(n, retries)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
