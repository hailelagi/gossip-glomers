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
}

func (s *session) sendHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offset := s.replicatedLog.Append(body["key"], body["msg"])
	return s.node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func (s *session) pollHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]any)
	msgs := s.replicatedLog.Read(offsets)

	return s.node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func (s *session) hasCommitHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	requestedOffsets := body["offsets"].(map[string]int)
	committed := s.replicatedLog.HasCommitted(requestedOffsets)

	if committed {
		return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	}

	// todo: return error
	return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func (s *session) listCommittedHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	keys := body["keys"].([]any)
	committed := s.replicatedLog.listCommitted(keys)

	return s.node.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": committed})
}

func main() {
	n := maelstrom.NewNode()
	replicatedlog := &replicatedLog{index: map[string][]int{}, log: []entry{}}

	s := &session{
		node:          n,
		replicatedLog: replicatedlog,
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
