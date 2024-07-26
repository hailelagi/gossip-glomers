package main

import (
	"log"
	"os"

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

// the operation is addition and is commutative
// as it is a counter that only ever grows

/*
func (s *session) addOperationHandler(msg maelstrom.Message) error {
	var wg sync.WaitGroup
	var result int
	var body map[string]any
	var atom sync.Mutex

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
	defer cancel()

	atom.Lock()
	previous, err := s.kv.Read(ctx, fmt.Sprint("counter-", s.node.ID()))

	if err != nil {
		result = 0
	} else {
		result = previous.(int) + delta
	}

	err = s.kv.CompareAndSwap(ctx, fmt.Sprint("counter-", s.node.ID()), previous, result, true)
	atom.Unlock()

	if err != nil {
		log.SetOutput(os.Stderr)
		log.Print(err)
	}

	return s.node.Reply(msg, map[string]any{"type": "add_ok"})
}

/*
func (s *session) readOperationHandler(msg maelstrom.Message) error {
	var body = map[string]any{"type": "read_ok"}
	var result int
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
	defer cancel()

	count, err := s.kv.ReadInt(ctx, fmt.Sprint("counter-", s.node.ID()))

	body["value"] = result
	return s.node.Reply(msg, body)
}
*/

/*
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
*/

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

	n.Handle("add", s.addStateTransformHandler)
	n.Handle("read", s.readStateTransformHandler)

	/*
		for i := 0; i < runtime.NumCPU(); i++ {
			go failureDetector(s)
		}
	*/

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
