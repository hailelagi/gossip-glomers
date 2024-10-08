package main

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type replicatedLog struct {
	committed map[string]float64
	version   map[string][]int
	log       []entry
	// pLocks    []*sync.RWMutex
	global sync.RWMutex
}

type entry struct {
	key    string
	value  float64
	offset float64
}

func NewLog(partitions int) *replicatedLog {
	// todo(optimization): implement available partitions
	// using a consistent hashing algorithm over entry key per len(nodes)
	// or more simply randomize
	/*
		    i := k % n.nodeID()
			for i := range locks {
				locks[i] = &sync.RWMutex{}
			}
	*/

	return &replicatedLog{
		committed: map[string]float64{},
		version:   map[string][]int{},
		// this will explode past, probably not a good idea
		// at some point, compaction should kick in, or instead of preallocating
		// the offsets are mapped and appended
		log:    make([]entry, 10_000_000),
		global: sync.RWMutex{},
	}
}

// This is ineffcient. In a real implementation
// this would be a CAS against an atomic pointer or an atomic CoW memswap
// for simplicity and sanity, a simple mutual exclusion lock is used
// obviously this contends the local lock on this service.
// Append a k/v entry to the log and returns the last index offset
func (l *replicatedLog) Append(offset int, key, value any) int {
	l.global.Lock()
	defer l.global.Unlock()

	k, v := key.(string), value.(float64)
	event := entry{key: k, value: v, offset: float64(offset)}

	l.log[offset] = event
	l.version[key.(string)] = append(l.version[key.(string)], offset)

	return offset
}

func (l *replicatedLog) acquireLease(kv *maelstrom.KV) int {
	l.global.Lock()
	defer l.global.Unlock()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
	var count int
	defer cancel()

	err := errors.New("busy wait")

	// without a consistently reachable k/v store the protocol breaks down
	// and cannot make progress
	for err != nil {
		previous, _ := kv.Read(ctx, "monotonic-counter")

		if previous == nil {
			previous = 0
			count = 1
		} else {
			count = previous.(int) + 1
		}

		err = kv.CompareAndSwap(ctx, "monotonic-counter", previous, count, true)
	}

	return count
}

// Read messages from a set of logs starting from the given offset in each log
func (l *replicatedLog) Read(offsets map[string]any) map[string][][]float64 {
	l.global.Lock()
	defer l.global.Unlock()

	var result = make(map[string][][]float64)

	for key, offset := range offsets {
		result[key] = l.seek(key, int(offset.(float64)))
	}

	return result
}

// Commit ack the last offset a client should read from by the server
func (l *replicatedLog) Commit(kv *maelstrom.KV, offsets map[string]any) {
	l.global.Lock()
	defer l.global.Unlock()

	for key, offset := range offsets {
		l.committed[key] = offset.(float64)
	}
}

// ListCommited view the current committed offsets ack'd by the server
func (l *replicatedLog) ListCommitted(keys []any) map[string]any {
	l.global.RLock()
	defer l.global.RUnlock()

	var offsets = make(map[string]any)

	for _, key := range keys {
		key := key.(string)
		offsets[key] = l.committed[key]
	}

	return offsets
}

func (l *replicatedLog) seek(key string, beginIdx int) [][]float64 {
	var result [][]float64
	history := l.version[key]

	// concurrent out of order updates messup the cache's history
	sort.Ints(history)

	start := sort.Search(len(history), func(i int) bool {
		return history[i] >= beginIdx
	})

	for i := start; i <= len(history)-1; i++ {
		offset := history[i]
		entry := l.log[offset]

		result = append(result, []float64{float64(offset), entry.value})
	}

	return result
}
