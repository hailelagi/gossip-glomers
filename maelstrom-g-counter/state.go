package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// State based transform + SeqCst store, the k/v store is a co-ordination point.
func (s *session) addStateSeqCstHandler(msg maelstrom.Message) error {
	var result int
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
	defer cancel()

	previous, err := s.kv.Read(ctx, fmt.Sprint("counter-", s.node.ID()))

	if err != nil {
		result = delta
	} else {
		result = previous.(int) + delta
	}

	err = s.kv.CompareAndSwap(ctx, fmt.Sprint("counter-", s.node.ID()), previous, result, true)

	if err != nil {
		log.SetOutput(os.Stderr)
		log.Print(err)
	}

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

// we need to merge
func (s *session) readStateSeqCstHandler(msg maelstrom.Message) error {
	var result int
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
	defer cancel()

	for _, n := range s.node.NodeIDs() {
		count, _ := s.kv.ReadInt(ctx, fmt.Sprint("counter-", n))

		result = result + count

	}

	return s.node.Reply(msg, map[string]any{"type": "read_ok", "value": result})
}
