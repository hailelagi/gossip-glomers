package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
// Operation based transform
1: payload Payload type; instantiated at all replicas
2: initial Initial value
3: query Source-local operation (arguments) : returns
4: pre Precondition
5: let Execute at source, synchronously, no side effects
6: update Global update (arguments) : returns
7: atSource (arguments) : returns
8: pre Precondition at source
9: let 1st phase: synchronous, at source, no side effects
10: downstream (arguments passed downstream)
11: pre Precondition against downstream state
12: 2nd phase, asynchronous, side-effects to downstream state
*/

func (s *session) addOperationHandler(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *session) readOperationHandler(msg maelstrom.Message) error {
	var body = map[string]any{"type": "read_ok"}
	var result int

	body["value"] = result
	return s.node.Reply(msg, body)
}
