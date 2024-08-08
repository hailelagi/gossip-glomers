package main

import (
	"sync"
)

type replicatedLog struct {
	committed map[string]float64
	version   map[string][]int
	log       []entry
	pLocks    []*sync.RWMutex
	global    sync.RWMutex
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
		committed: map[string]float64{},
		version:   map[string][]int{},
		log:       []entry{},
		pLocks:    locks,
		global:    sync.RWMutex{},
	}
}

// Append a k/v entry to the log and returns the last index offset
func (l *replicatedLog) Append(key string, value float64) int {
	l.global.Lock()
	defer l.global.Unlock()
	var event entry

	offset := len(l.log) + 1
	event = entry{key: key, value: value, offset: offset}

	l.log = append(l.log, event)
	l.version[key] = append(l.version[key], offset)

	return offset
}

// Read messages from a set of logs starting from the given offset in each log
func (l *replicatedLog) Read(offsets map[string]float64) map[string][][]int {
	l.global.RLock()
	defer l.global.RUnlock()

	var result = make(map[string][][]int)

	for key, offset := range offsets {
		result[key] = l.seek(key, int(offset))
	}

	return result
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

		if offset != entry.offset {
			panic("entry not in log")
		}

		result = append(result, []int{offset, int(entry.value)})

	}

	return result
}

// Commit informs the server of the client's 'processed' offset
func (l *replicatedLog) Commit(offsets map[string]float64) {
	l.global.Lock()
	defer l.global.Lock()

	for key, offset := range offsets {
		l.committed[key] = offset
	}
}

// ListCommitted returns the 'latest' committed/processed offset for clients
func (l *replicatedLog) ListCommitted(keys []any) map[string]any {
	l.global.RLock()
	defer l.global.RUnlock()

	var result = make(map[string]any)

	for _, key := range keys {
		key := key.(string)
		result[key] = l.committed[key]
	}

	return result
}
