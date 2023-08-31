package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var store []int

	n.Handle("broadcast", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Store the message in body["message"]
		store = append(store, body["message"].(int))
		body["type"] = "broadcast_ok"

		for _, node := range n.NodeIDs() {
			n.Send(node, body)
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

		for _, node := range n.NodeIDs() {
			return n.RPC(node, body, func(msg maelstrom.Message) error {
				return nil
			})
		}
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "topology_ok"

		for _, node := range n.NodeIDs() {
			n.Send(node, body)
		}

		return nil
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
