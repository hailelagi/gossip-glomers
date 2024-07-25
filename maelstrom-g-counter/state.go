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

func (s *session) addStateTransformHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var result int
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	previous, err := s.kv.Read(ctx, fmt.Sprint("counter-", s.node.ID()))

	if err != nil {
		result = 0
	} else {
		result = previous.(int) + delta
	}

	err = s.kv.CompareAndSwap(ctx, fmt.Sprint("counter-", s.node.ID()), previous, result, true)

	if err != nil {
		log.SetOutput(os.Stderr)
		log.Print(err)
	}

	for _, dest := range s.node.NodeIDs() {
		if dest == msg.Src || dest == s.node.ID() {
			continue
		}

		wg.Add(1)

		go func(dest string) {
			deadline := time.Now().Add(400 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			defer wg.Done()

			_, err := s.node.SyncRPC(ctx, dest, body)

			if err == nil {
				return
			} else {
				s.retries <- retry{body: body, dest: dest, attempt: 20, err: err}
			}
		}(dest)
	}

	wg.Wait()

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *session) readStateTransformHandler(msg maelstrom.Message) error {
	var body = map[string]any{"type": "read_ok"}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	count, err := s.kv.ReadInt(ctx, fmt.Sprint("counter-", s.node.ID()))

	if err != nil {
		count = 0
	}

	body["value"] = count
	return s.node.Reply(msg, body)
}
