package main

import maelstrom "github.com/jepsen-io/maelstrom/demo/go"

/*
// State based transform
1: payload Payload type; instantiated at all replicas
2: initial Initial value
3: query Query (arguments) : returns
4: pre Precondition
5: let Evaluate synchronously, no side effects
6: update Source-local operation (arguments) : returns
7: pre Precondition
8: let Evaluate at source, synchronously
9: Side-effects at source to execute synchronously
10: compare (value1, value2) : boolean b
11: Is value1 ≤ value2 in semilattice?
12: merge (value1, value2) : payload mergedValue
13: LUB merge of value1 and value2, at any replica
*/

func (s *session) addStateHandler(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *session) readStateHandler(msg maelstrom.Message) error {
	var body = map[string]any{"type": "read_ok"}
	var result int

	body["value"] = result
	return s.node.Reply(msg, body)
}
