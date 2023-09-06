package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastMessage struct {
	Message int `json:message`
}

type topologyMessage struct {
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()

	// todo: wrap store with mutex across concurrenct sessions
	var store []float64

	n.Handle("broadcast", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Store the message in body["message"]
		store = append(store, body["message"].(float64))
		body["type"] = "broadcast_ok"

		// TODO: make async/parallelise with go routines
		for _, node := range n.NodeIDs() {
			if node != n.ID() {
				n.Send(node, body)
			}
		}

		return n.Reply(msg, body)
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

		var body topologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

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
