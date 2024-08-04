package main

import (
	"sync"
)

type replicatedLog struct {
	index map[string][]int
	log   []entry
	sync.RWMutex
}

type entry struct {
	key   string
	value float64
	// offset int
}

// Append a k/v entry to the log and returns the last index offset
func (l *replicatedLog) Append(key, value any) int {
	event := entry{key: key.(string), value: value.(float64)}
	index := l.index[key.(string)]

	l.log = append(l.log, event)
	offset := len(l.log) - 1
	_ = append(index, offset)

	return offset
}

// Read messages from a set of logs starting from the given offset in each log
func (l *replicatedLog) Read(offsets map[string]any) map[string]any {
	var result = make(map[string]any)

	for key, offset := range offsets {
		result[key] = l.seek(key, int(offset.(float64)))
	}

	return result
}

/*
func (l *replicatedLog) Read(key any) any {
	start, end := 0, len(l.log)-1

	for start <= end {
		mid := start + (end-start)/2

		if l.log[mid] == key {
			return mid
		} else if l.log[mid] < key {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}

	return -1
}
*/

func (l *replicatedLog) seek(key string, beginOffset int) [][]float64 {
	var result [][]float64

	for _, offset := range l.index[key] {
		if offset >= beginOffset {
			entry := l.log[offset]

			result = append(result, []float64{float64(offset), entry.value})
		}

	}

	return result
}

// HasCommitted Checks if the client and log are in sync
func (l *replicatedLog) HasCommitted(offsets map[string]int) bool {
	var IsCommitted bool

	for key, offset := range offsets {
		for _, k := range l.index[key] {
			if k == offset {
				IsCommitted = true
			} else {
				break
			}
		}
	}

	return IsCommitted
}

func (l *replicatedLog) listCommitted(keys []any) map[string]any {
	var result = make(map[string]any)

	for _, key := range keys {
		key := key.(string)

		if nil == l.index[key] {
			continue
		}

		result[key] = l.index[key][0]
	}

	return result
}
