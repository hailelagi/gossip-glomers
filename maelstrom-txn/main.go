package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type session struct {
	node *maelstrom.Node
	kv   *store
}

func (s *session) transactionHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	result := s.kv.newTxn(body["txn"].([]any))

	body["txn"] = result
	body["type"] = "txn_ok"
	return s.node.Reply(msg, body)
}

func main() {
	n := maelstrom.NewNode()

	s := &session{
		node: n,
		kv: &store{index: map[int]int{},
			log: make([]float64, 1000_000)},
	}

	n.Handle("txn", s.transactionHandler)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
