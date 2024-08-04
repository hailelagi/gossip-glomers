package main

import (
	"sync"
)

type replicatedLog struct {
	version map[string][]int
	log     []entry
	pLocks  []*sync.RWMutex
	global  sync.RWMutex
}

type entry struct {
	key    string
	value  float64
	offset int
}

func NewLog(partitions int) *replicatedLog {
	locks := make([]*sync.RWMutex, partitions)

	for i := range locks {
		locks[i] = &sync.RWMutex{}
	}

	return &replicatedLog{
		version: map[string][]int{},
		log:     []entry{},
		pLocks:  locks,
		global:  sync.RWMutex{},
	}
}

// Append a k/v entry to the log and returns the last index offset
func (l *replicatedLog) Append(key, value any) int {
	l.global.Lock()
	defer l.global.Unlock()

	offset := len(l.log) + 1

	event := entry{key: key.(string), value: value.(float64), offset: offset}
	l.log = append(l.log, event)
	l.version[key.(string)] = append(l.version[key.(string)], offset)

	return offset
}

// Read messages from a set of logs starting from the given offset in each log
func (l *replicatedLog) Read(offsets map[string]any) map[string][][]float64 {
	l.global.RLock()
	defer l.global.RUnlock()

	var result = make(map[string][][]float64)

	for key, offset := range offsets {
		result[key] = l.seek(key, int(offset.(float64)))
	}

	return result
}

func (l *replicatedLog) seek(key string, beginIdx int) [][]float64 {
	var result [][]float64
	/*
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
	*/

	for _, offset := range l.version[key] {
		if offset >= beginIdx {
			entry := l.log[offset-1]

			result = append(result, []float64{float64(offset), entry.value})
		}
	}

	return result
}

// HasCommitted Checks if the client and log are in sync
func (l *replicatedLog) HasCommitted(offsets map[string]any) bool {
	l.global.RLock()
	defer l.global.RUnlock()

	var IsCommitted bool

	for key, offset := range offsets {
		for _, k := range l.version[key] {
			if k == int(offset.(float64)) {
				IsCommitted = true
			} else {
				IsCommitted = false
				break
			}
		}
	}

	return IsCommitted
}

// ListCommitted returns the last offset for a client
func (l *replicatedLog) ListCommitted(keys []any) map[string]any {
	l.global.RLock()
	defer l.global.RUnlock()

	var result = make(map[string]any)

	for _, key := range keys {
		key := key.(string)
		version := l.version[key]

		if len(version) == 0 {
			continue
		}

		lastPosition := len(version) - 1
		result[key] = version[lastPosition]
	}

	return result
}
