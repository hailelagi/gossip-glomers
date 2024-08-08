package main

import "sync"

type replicatedLog struct {
	committed map[string]float64
	version   map[string][]int
	log       []entry
	pLocks    []*sync.RWMutex
	global    sync.RWMutex
}

type entry struct {
	key   string
	value float64
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
func (l *replicatedLog) Append(key, value any) int {
	l.global.Lock()
	defer l.global.Unlock()

	offset := len(l.log) + 1

	event := entry{key: key.(string), value: value.(float64)}
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

func (l *replicatedLog) Commit(offsets map[string]any) {
	l.global.Lock()
	defer l.global.Unlock()

	for key, offset := range offsets {
		l.committed[key] = offset.(float64)
	}
}

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

	for _, offset := range l.version[key] {
		if offset >= beginIdx {
			entry := l.log[offset-1]

			result = append(result, []float64{float64(offset), entry.value})
		}
	}

	return result
}
