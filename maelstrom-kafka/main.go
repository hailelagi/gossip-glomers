package main

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type replicatedLog struct {
	committed map[string]int
	version   map[string][]int
	log       []entry
	pLocks    []*sync.RWMutex
	global    sync.RWMutex
}

type entry struct {
	key   string
	value float64
}

type session struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
	log  *replicatedLog
}

type Body struct {
	MsgId   int            `json:"msg_id"`
	Offsets map[string]int `json:"offsets"`
	Type    string         `json:"type"`
}

func (s *session) sendHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	l := s.log
	l.global.Lock()
	defer l.global.Unlock()
	var event entry

	offset := len(l.log) + 1
	key := body["key"].(string)
	value := body["msg"].(float64)

	event = entry{key: key, value: value}
	l.log = append(l.log, event)
	l.version[key] = append(l.version[key], offset)

	return s.node.Reply(msg, map[string]any{"type": "send_ok", "offset": float64(offset)})
}

func (s *session) pollHandler(msg maelstrom.Message) error {
	var body Body

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	l := s.log
	l.global.RLock()
	defer l.global.RUnlock()

	var msgs = make(map[string][][]int)

	for key, offset := range body.Offsets {
		msgs[key] = l.seek(key, int(offset))
	}

	log.SetOutput(os.Stderr)
	log.Printf("wtf? %v", body.Offsets)

	return s.node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func (s *session) CommitOffsetHandler(msg maelstrom.Message) error {
	var body Body

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	l := s.log
	l.global.Lock()
	defer l.global.Lock()

	for key, offset := range body.Offsets {
		l.committed[key] = offset
	}

	return s.node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})

}

func (s *session) listCommittedHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	keys := body["keys"].([]any)
	l := s.log

	l.global.RLock()
	defer l.global.RUnlock()

	var offsets = make(map[string]any)

	for _, key := range keys {
		key := key.(string)
		offsets[key] = l.committed[key]
	}

	return s.node.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
}

func (l *replicatedLog) seek(key string, beginIdx int) [][]int {
	var result [][]int
	var position int
	var found bool

	history := l.version[key]
	start, end := 0, len(history)-1

	for start <= end {
		mid := start + (end-start)/2

		if history[mid] == beginIdx {
			found = true
			position = mid
			break
		} else if history[mid] < beginIdx {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}

	if !found {
		return nil
	}

	for _, offset := range history[position:] {
		entry := l.log[offset-1]
		result = append(result, []int{offset, int(entry.value)})

	}

	return result
}

func NewLog(partitions int) *replicatedLog {
	locks := make([]*sync.RWMutex, partitions)

	for i := range locks {
		locks[i] = &sync.RWMutex{}
	}

	return &replicatedLog{
		committed: map[string]int{},
		version:   map[string][]int{},
		log:       []entry{},
		pLocks:    locks,
		global:    sync.RWMutex{},
	}
}

func main() {
	n := maelstrom.NewNode()

	s := &session{
		node: n,
		kv:   maelstrom.NewLinKV(n),
		log:  NewLog(runtime.NumCPU()),
	}

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
