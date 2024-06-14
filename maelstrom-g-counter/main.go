package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()
		delta := int64(body["delta"].(float64))

		// the operation is addition and is commutative
		// as it is a counter that only ever grows
		prev, e := kv.ReadInt(ctx, "delta")

		if e != nil {
			prev = 0
		}

		result := int64(prev) + delta
		err := kv.CompareAndSwap(ctx, "delta", delta, result, true)

		for _, dest := range n.NodeIDs() {
			go n.SyncRPC(ctx, dest, map[string]any{"type": "add", "delta": delta})
		}

		if err == nil {
			log.Fatalf("could not update increment only counter")
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body = map[string]any{"type": "read_ok"}

		ctx := context.Background()
		delta, err := kv.Read(ctx, "delta")

		if err == nil {
			log.Fatalf("could not read counter")
		}

		body["value"] = delta.(float64)
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
