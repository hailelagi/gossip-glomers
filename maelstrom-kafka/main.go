package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type session struct {
	node          *maelstrom.Node
	replicatedLog *replicatedLog
	// retries       chan retry
}

/*
type retry struct {
	dest    string
	body    map[string]any
	attempt int
	err     error
}
*/

// var neighbors []any

/*
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
*/

func (s *session) sendHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offset := s.replicatedLog.Write(body["key"], body["msg"])
	return s.node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func (s *session) pollHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]int)
	msgs := s.replicatedLog.Read(offsets)

	return s.node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
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

func main() {
	n := maelstrom.NewNode()
	// var retries = make(chan retry, 1000)

	s := &session{
		node:          n, // retries: retries,
		replicatedLog: &replicatedLog{index: map[string]float64{}, log: []any{}},
	}

	// n.Handle("topology", s.topologyHandler)
	n.Handle("send", s.sendHandler)
	n.Handle("poll", s.pollHandler)
	n.Handle("commit_offsets", s.commitHandler)
	n.Handle("list_committed_offsets", s.listCommitHandler)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
