package main

import (
	"context"
	"encoding/json"
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
	log     *replicatedLog
	retries chan retry
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

func (s *session) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	var wg sync.WaitGroup

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// reserve a monotonic count slot
	offset := s.log.acquireLease(s.kv)

	for _, dest := range s.node.NodeIDs() {
		wg.Add(1)

		go func(dest string) {
			deadline := time.Now().Add(400 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			replicaBody := map[string]any{
				"type": "replicate", "offset": offset,
				"key": body["key"], "msg": body["msg"],
			}

			defer cancel()
			defer wg.Done()

			_, err := s.node.SyncRPC(ctx, dest, replicaBody)

			if err == nil {
				return
			} else {
				s.retries <- retry{body: replicaBody, dest: dest, attempt: 20, err: err}
			}
		}(dest)
	}

	wg.Wait()

	return s.node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func (s *session) replicateHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offset, key, value := body["offset"], body["key"], body["msg"]
	s.log.Append(int(offset.(float64)), key.(string), value)

	return s.node.Reply(msg, map[string]any{"type": "replicate_ok"})
}

func (s *session) pollHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgs := s.log.Read(body["offsets"].(map[string]any))
	return s.node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func (s *session) CommitOffsetsHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.log.Commit(s.kv, body["offsets"].(map[string]any))
	return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func (s *session) listCommittedHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := s.log.ListCommitted(s.kv, body["keys"].([]any))
	return s.node.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
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

	s := &session{
		node: n,
		kv:   maelstrom.NewLinKV(n),
		log:  NewLog(runtime.NumCPU()),
	}

	n.Handle("send", s.sendHandler)
	n.Handle("replicate", s.replicateHandler)
	n.Handle("poll", s.pollHandler)
	n.Handle("commit_offsets", s.CommitOffsetsHandler)
	n.Handle("list_committed_offsets", s.listCommittedHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
