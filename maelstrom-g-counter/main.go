package main

import (
	"context"
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

func failureDetector(s *session) {
	var atttempts sync.WaitGroup

	for r := range s.retries {
		r := r

		if r.dest == r.body["src"] || r.dest == s.node.ID() {
			continue
		}

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
				} else {
					s.retries <- retry
				}

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
	var retries = make(chan retry, 1000)

	s := &session{
		node: n, retries: retries,
		kv: kv,
	}

	/*
		n.Handle("add", s.addOperationHandler)
		n.Handle("read", s.readOperationHandler)
	*/

	n.Handle("add", s.addStateSeqCstHandler)
	n.Handle("read", s.readStateSeqCstHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
