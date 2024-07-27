package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type session struct {
	node *maelstrom.Node
	kv   *store
}

type store struct {
	index map[int]int
	log   []float64
	sync.RWMutex
}

func (s *session) transactionHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	kv := s.kv
	kv.Lock()
	defer kv.Unlock()

	txn := body["txn"].([]any)
	var result = make([][]any, 0)

	for _, op := range txn {
		op := op.([]any)

		if op[0] == "r" {
			index := op[1].(float64)

			result = append(result, []any{"r", index, kv.log[int(index)]})
		} else if op[0] == "w" {
			index := op[1].(float64)
			value := op[2].(float64)
			kv.log[int(index)] = value

			result = append(result, []any{"w", index, kv.log[int(index)]})
		}
	}

	body["txn"] = result
	body["type"] = "txn_ok"
	return s.node.Reply(msg, body)
}

func main() {
	n := maelstrom.NewNode()

	s := &session{
		node: n,
		kv:   &store{index: map[int]int{}, log: make([]float64, 1000_000)},
	}

	n.Handle("txn", s.transactionHandler)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
