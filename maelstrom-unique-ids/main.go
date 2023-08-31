package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		// body["id"] = genUUID()
		body["id"] = genNaiveUUID(n.ID())

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

// Solution one, use a UUID with a very large key space (2 ** 128 - 1)
// UUID's use a more sophisticated version the naive version below
// https://datatracker.ietf.org/doc/html/rfc4122
func genUUID() string {
	return uuid.NewString()
}

// Solution two, unique non-monotonically, not synced increasing function
// The clock can skew here but that's okay for this challenge
// it's also difficult to impose true order because the requestID
// is random and doesn't know which timestamp came first.
// But it's good enough :)
func genNaiveUUID(nodeID string) int64 {
	requestID := strconv.FormatInt(rand.Int63n(100), 10)
	sequenceId := strconv.FormatInt(time.Now().UnixMicro(), 10)
	originId := nodeID[1:]

	identity := originId + requestID + sequenceId

	if id, err := strconv.ParseInt(identity, 10, 64); err != nil {
		log.Fatal(err)
		return 0
	} else {
		return id
	}
}
