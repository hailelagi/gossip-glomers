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

type Store struct {
	index map[float64]bool
	log   []float64
	sync.RWMutex
}

type Session struct {
	node  *maelstrom.Node
	store *Store
}

type Retry struct {
	dest    string
	body    map[string]any
	attempt int
	exec    func(context.Context, string, any) (maelstrom.Message, error)
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

	store.Lock()
	_, exists := store.index[key]

	if !exists {
		store.index[key] = true
		store.log = append(store.log, key)
	}
	store.Unlock()

	if exists {
		body["type"] = "broadcast_ok"
		delete(body, "message")

		return s.node.Reply(msg, body)
	} else {
		retries := make(chan Retry, len(n.NodeIDs())-1)

		for _, dest := range n.NodeIDs() {
			if dest != n.ID() {
				wg.Add(1)

				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
				defer cancel()

				go func(dest string) {
					defer wg.Done()
					_, err := n.SyncRPC(ctx, dest, body)

					if err == nil {
						return
					} else {
						retries <- Retry{body: body, dest: dest, attempt: 10, exec: n.SyncRPC, err: err}
					}

				}(dest)
			}
		}

		wg.Wait()

		go func() {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
			defer cancel()

			for retry := range retries {
				if retry.attempt > 0 {
					retry.attempt--
					_, err := retry.exec(ctx, retry.dest, retry.body)

					if err == nil {
					} else {
						retries <- retry
						time.Sleep(10 * time.Millisecond)
					}
				} else {
					continue
				}
			}
		}()

		// ack
		if body["msg_id"] == nil {
			return nil
		} else {
			body["type"] = "broadcast_ok"
			delete(body, "message")
			return s.node.Reply(msg, body)
		}

	}
}

func main() {
	n := maelstrom.NewNode()
	s := &Session{node: n, store: &Store{index: map[float64]bool{}, log: []float64{}}}

	n.Handle("topology", s.topologyHandler)
	n.Handle("read", s.readHandler)
	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
