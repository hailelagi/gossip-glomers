package main

import (
	"encoding/json"
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
	// The value is always an integer and it is unique for each message from Maelstrom
	msgs []any
	mu   sync.RWMutex
}

var store Store
var wg sync.WaitGroup

func main() {
	n := maelstrom.NewNode()
	// results := make(chan error, len(n.NodeIDs()))

	n.Handle("topology", func(msg maelstrom.Message) error {
		// TODO: diy yourself a topology of known 'logical' nodes that are discoverable
		// this allows us to to more efficient send messages
		var body = make(map[string]any)
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msg_id := body["msg_id"]
		delete(body, "msg_id")
		key := body["message"]

		exists := false
		store.mu.RLock()

		for _, v := range store.msgs {
			if key == v {
				exists = true
				break
			}
		}
		store.mu.RUnlock()

		if exists {
			// ack
			body["type"] = "broadcast_ok"
			delete(body, "message")
			return n.Reply(msg, body)
		} else {
			// store locally
			store.mu.Lock()
			store.msgs = append(store.msgs, key)
			store.mu.Unlock()

			for _, dest := range n.NodeIDs() {
				if dest != n.ID() {
					wg.Add(1)

					go func(dest string) {
						defer wg.Done()
						err := n.Send(dest, body)

						if err != nil {
							for i := 1; i <= 5; i++ {
								err := n.Send(dest, body)
								if err == nil {
									return
								}
								time.Sleep(time.Duration(i+1) * time.Second)
							}

							return
						}
					}(dest)
				}
			}

			wg.Wait()

			// ack
			if msg_id == nil {
				return nil
			} else {
				body["type"] = "broadcast_ok"
				delete(body, "message")
				return n.Reply(msg, body)
			}

		}
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		store.mu.RLock()
		defer store.mu.RUnlock()

		body["type"] = "read_ok"
		body["messages"] = store.msgs

		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
