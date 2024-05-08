package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
theory/explaination overall concept family "Broadcast alogrithms", distributed algorithm: "gossip protocol":
- https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf
- https://www.youtube.com/watch?v=77qpCahU3fo

read more:
van Steen & Tanenbaum: https://www.distributed-systems.net/index.php/books/ds4/

DDIA:
Cassandra and Riak take a different approach: they use a gossip protocol among the nodes to disseminate any changes
in cluster state. Requests can be sent to any node, and that node forwards them to the appropriate node for the requested partition
*/

type Store struct {
	// The value is always an integer and it is unique for each message from maelstrom
	set  map[float64]bool
	msgs []float64
	sync.RWMutex
}

type Session struct {
	node  *maelstrom.Node
	store *Store
	wg    sync.WaitGroup
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
	body["messages"] = s.store.msgs

	return s.node.Reply(msg, body)
}

func (s *Session) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	var store = s.store
	n := s.node

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["message"].(float64)

	store.Lock()
	_, exists := store.set[key]

	if !exists {
		store.set[key] = true
		store.msgs = append(store.msgs, key)
	}
	store.Unlock()

	if exists {
		body["type"] = "broadcast_ok"
		delete(body, "message")

		return s.node.Reply(msg, body)
	} else {
		retry := make(chan error, 5)

		for _, dest := range n.NodeIDs() {
			if dest != n.ID() {
				s.wg.Add(1)

				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
				defer cancel()

				go func(dest string) {
					defer s.wg.Done()
					_, err := n.SyncRPC(ctx, dest, body)

					if err == nil {
						return
					}

					retry <- err

				}(dest)
			}
		}

		fmt.Fprintln(os.Stderr, <-retry)
		s.wg.Wait()
		fmt.Fprintln(os.Stderr, <-retry)

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
	s := &Session{node: n, store: &Store{set: map[float64]bool{}, msgs: []float64{}}}

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
