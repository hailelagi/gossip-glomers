package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
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
	node  *maelstrom.Node
	store *store
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

/*
todo: use little's law: L = rate * wait time
and somewhat estimate a 'reasonable' queue size
*/
var retries = make(chan retry, 500)

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
	var store = s.store
	n := s.node

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["message"].(float64)
	resp := map[string]any{"type": "broadcast_ok", "msg_id": body["msg_id"]}
	exists := store.findOrInsert(key)

	if exists {
		return nil
	}

	for _, dest := range n.NodeIDs() {
		wg.Add(1)

		deadline := time.Now().Add(400 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		go func(dest string) {
			defer wg.Done()
			_, err := n.SyncRPC(ctx, dest, body)

			if err == nil {
				return
			} else {
				retries <- retry{body: body, dest: dest, attempt: 20, err: err}
			}
		}(dest)
	}

	wg.Wait()

	return s.node.Reply(msg, resp)
}

func failureDetector(n *maelstrom.Node, c *sync.Cond, retries chan retry) {
	c.L.Lock()

	for r := range retries {
		c.Wait()

		go func(retry retry) {
			deadline := time.Now().Add(400 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()

			retry.attempt--

			if retry.attempt >= 0 {
				_, err := n.SyncRPC(ctx, retry.dest, retry.body)

				if err != nil {
					retries <- retry
				}
			} else {
				log.SetOutput(os.Stderr)
				log.Printf("message slip loss beyond tolerance from queue %v", retry)
			}
		}(r)
	}

	c.L.Unlock()
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
	defer close(retries)

	s := &session{node: n, store: &store{index: map[float64]bool{}, log: []float64{}}}

	n.Handle("topology", s.topologyHandler)
	n.Handle("read", s.readHandler)
	n.Handle("broadcast", s.broadcastHandler)

	c := sync.NewCond(&sync.Mutex{})
	// background failure detection
	go failureDetector(n, c, retries)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

}
