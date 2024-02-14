package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
further reading - theory/explaination key word "Broadcast alogrithms", production systems "gossip protocol":
- https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf
- https://www.youtube.com/watch?v=77qpCahU3fo

indepth:
van Steen & Tanenbaum: https://www.distributed-systems.net/index.php/books/ds4/

DDIA:
Cassandra and Riak take a different approach: they use a gossip protocol among the nodes to disseminate any changes
in cluster state. Requests can be sent to any node, and that node forwards them to the appropriate node for the requested partition
*/

func main() {
	n := maelstrom.NewNode()

	// The value is always an integer and it is unique for each message from Maelstrom
	var store []int64

	// gossip/pubsub init
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Store the message in body["message"]
		store = append(store, body["message"].(int64))
		body["type"] = "broadcast_ok"

		for _, node := range n.NodeIDs() {
			if node != n.ID() {
				n.Send(node, body)
			}
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = store

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		res := make(map[string]interface{})
		res["type"] = "topology_ok"
		return n.Reply(msg, res)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
