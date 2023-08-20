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
	logId := strconv.FormatInt(rand.Int63n(100), 10)

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = genSnowFlake(logId, n.ID())

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

// Solution one, use a UUID with a very large key space (2 ** 128 - 1)
func genUUID() string {
	return uuid.NewString()
}

// Solution two, use a UUID with a very large key space
func genSnowFlake(logID string, nodeID string) string {
	sequenceId := strconv.FormatInt(time.Now().UnixMilli(), 10)
	return nodeID + sequenceId + logID

	// if id, err := strconv.ParseInt(sequenceId+logID, 10, 64); err != nil {
	// 	return
	// } else {
	// 	return id
	// }

}

func consistentHashRing() {

}
