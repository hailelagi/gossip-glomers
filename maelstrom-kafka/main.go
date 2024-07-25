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

type store struct {
	index map[float64]bool
	log   []float64
	sync.RWMutex
}

type session struct {
	node    *maelstrom.Node
	store   *store
	retries chan retry
}

type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}

var neighbors []any

// the replicated log
// TODO: how to model offsets in the log?
// can we just use slice indices?
var replicatedLog []map[string]int

func (s *session) topologyHandler(msg maelstrom.Message) error {
	var body = make(map[string]any)

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	self := s.node.ID()
	topology := body["topology"].(map[string]any)
	neighbors = topology[self].([]any)

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *session) sendHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	//log = append(log, body["msg"])

	return s.node.Reply(msg, map[string]any{"type": "send_ok", "msg_id": body["msg_id"]})
}
func (s *session) pollHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{"type": "poll_ok", "msg_id": body["msg_id"]})
}

func (s *session) commitHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{"type": "commit_offsets", "msg_id": body["msg_id"]})
}

func (s *session) listCommitHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{"type": "commit_offsets", "msg_id": body["msg_id"]})
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
	var retries = make(chan retry, 1000)

	s := &session{
		node: n, retries: retries,
		store: &store{index: map[float64]bool{}, log: []float64{}},
	}

	n.Handle("topology", s.topologyHandler)
	n.Handle("send", s.sendHandler)
	n.Handle("poll", s.pollHandler)
	n.Handle("commit_offsets", s.commitHandler)
	n.Handle("list_committed_offsets", s.listCommitHandler)

	for i := 0; i < runtime.NumCPU(); i++ {
		go failureDetector(s)
	}

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
