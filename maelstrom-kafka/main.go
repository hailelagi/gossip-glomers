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

	offset := s.log.Append(body["key"].(string), body["msg"].(float64))
	return s.node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func (s *session) pollHandler(msg maelstrom.Message) error {
	var body map[string]any
	var offsets map[string]float64

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offRaw, _ := json.Marshal(body["offsets"])
	if err := json.Unmarshal(offRaw, &offsets); err != nil {
		return err
	}

	msgs := s.log.Read(offsets)

	log.SetOutput(os.Stderr)
	log.Printf("wtf? %v", offsets)

	return s.node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func (s *session) CommitOffsetHandler(msg maelstrom.Message) error {
	var body map[string]any
	var offsets map[string]float64

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offRaw, _ := json.Marshal(body["offsets"])
	if err := json.Unmarshal(offRaw, &offsets); err != nil {
		return err
	}

	s.log.Commit(offsets)
	return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})

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
	n.Handle("commit_offsets", s.CommitOffsetHandler)
	n.Handle("list_committed_offsets", s.listCommittedHandler)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
