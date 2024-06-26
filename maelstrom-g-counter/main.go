package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type session struct {
	node    *maelstrom.Node
	kv      *maelstrom.KV
	retries chan retry
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

// the operation is addition and is commutative
// as it is a counter that only ever grows
func (s *session) addOperationHandler(msg maelstrom.Message) error {
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

func (s *session) readOperationHandler(msg maelstrom.Message) error {
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

func failureDetector(s *session) {
	var atttempts sync.WaitGroup

	for r := range s.retries {
		r := r
		atttempts.Add(1)

		go func(retry retry, attempts *sync.WaitGroup) {
			deadline := time.Now().Add(800 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			defer attempts.Done()

			retry.attempt--

			if retry.attempt >= 0 {
				_, err := s.node.SyncRPC(ctx, retry.dest, retry.body)

				if err == nil {
					return
				}
				s.retries <- retry

			} else {
				log.SetOutput(os.Stderr)
				log.Printf("dead letter message slip loss beyond tolerance %v", retry)
			}
		}(r, &atttempts)
	}

	atttempts.Wait()
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	var retries = make(chan retry, 100)

	s := &session{
		node: n, retries: retries,
		kv: kv,
	}

	n.Handle("add", s.addOperationHandler)
	n.Handle("read", s.readOperationHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
