package main

import (
	"encoding/json"
	"log"
	"os"
	"runtime"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type session struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
	log  *replicatedLog
}

func (s *session) sendHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offset := s.log.Append(body["key"], body["msg"])
	return s.node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func (s *session) pollHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]any)
	msgs := s.log.Read(offsets)

	return s.node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func (s *session) hasCommitHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	requestedOffsets := body["offsets"].(map[string]int)
	committed := s.log.HasCommitted(requestedOffsets)

	if committed {
		return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	}

	return nil
}

func (s *session) listCommittedHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	keys := body["keys"].([]any)
	committed := s.log.ListCommitted(keys)

	return s.node.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": committed})
}

func main() {
	n := maelstrom.NewNode()

	s := &session{
		node: n,
		kv:   maelstrom.NewLinKV(n),
		log:  NewLog(runtime.NumCPU()),
	}

	// n.Handle("topology", s.topologyHandler)
	n.Handle("send", s.sendHandler)
	n.Handle("poll", s.pollHandler)
	n.Handle("commit_offsets", s.hasCommitHandler)
	n.Handle("list_committed_offsets", s.listCommittedHandler)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
