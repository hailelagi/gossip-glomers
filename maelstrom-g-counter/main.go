package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
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

var gCounter int64

func (s *session) addHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// ctx := context.Background()
	delta := int64(body["delta"].(float64))
	atomic.AddInt64(&gCounter, delta)

	// the operation is addition and is commutative
	// as it is a counter that only ever grows
	// prev, e := s.kv.ReadInt(ctx, "counter")
	/*
		if e != nil {
			prev = 0
		}

		result := int64(prev) + delta
		err := s.kv.CompareAndSwap(ctx, "counter", delta, result, true)
	*/
	for _, dest := range s.node.NodeIDs() {
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

	/*
		if err == nil {
			log.Fatalf("could not update increment only counter")
		}
	*/

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *session) readHandler(msg maelstrom.Message) error {
	var body = map[string]any{"type": "read_ok"}

	/*
		ctx := context.Background()
		delta, err := s.kv.Read(ctx, "counter")

		if err == nil {
			log.Fatalf("could not read counter")
		}

		body["value"] = delta.(float64)
	*/

	delta := atomic.LoadInt64(&gCounter)
	body["value"] = delta

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

	n.Handle("add", s.addHandler)
	n.Handle("read", s.readHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
