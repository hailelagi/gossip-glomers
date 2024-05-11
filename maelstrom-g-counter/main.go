package main

import (
	"encoding/json"
	"log"
	"os"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var gCounter int64

func main() {
	n := maelstrom.NewNode()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int64(body["delta"].(float64))
		atomic.AddInt64(&gCounter, delta)

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body = map[string]any{"type": "read_ok"}

		delta := atomic.LoadInt64(&gCounter)
		body["value"] = delta

		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
