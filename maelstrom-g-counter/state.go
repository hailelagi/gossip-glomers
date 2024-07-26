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

/*
// State based transform
1: payload Payload type; instantiated at all replicas
2: initial Initial value
3: query Query (arguments) : returns
4: pre Precondition
5: let Evaluate synchronously, no side effects
6: update Source-local operation (arguments) : returns
7: pre Precondition
8: let Evaluate at source, synchronously
9: Side-effects at source to execute synchronously
10: compare (value1, value2) : boolean b
11: Is value1 ≤ value2 in semilattice?
12: merge (value1, value2) : payload mergedValue
13: LUB merge of value1 and value2, at any replica
*/

func (s *session) addStateTransformHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
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
		result = 0
	} else {
		result = previous.(int) + delta
	}

	err = s.kv.Write(ctx, fmt.Sprint("counter-", s.node.ID()), result)

	if err != nil {
		log.SetOutput(os.Stderr)
		log.Print(err)
	}

	wg.Wait()

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

// we need to merge
func (s *session) readStateTransformHandler(msg maelstrom.Message) error {
	var result int
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	for _, n := range s.node.NodeIDs() {
		count, err := s.kv.ReadInt(ctx, fmt.Sprint("counter-", n))

		if err == nil {
			result = result + count
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "read_ok", "value": result})
}
