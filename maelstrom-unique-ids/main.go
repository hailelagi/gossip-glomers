package main

import (
	"encoding/json"
	uuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	n := maelstrom.NewNode()
	nodeId := n.ID()
	logId := strconv.FormatInt(rand.Int63n(100), 10)

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		sequenceId := strconv.FormatInt(time.Now().UnixMilli(), 10)
		body["type"] = "generate_ok"
		body["id"] = genUniqueID() + nodeId + logId + sequenceId

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func genUniqueID() string {
	return uuid.NewString()
}
