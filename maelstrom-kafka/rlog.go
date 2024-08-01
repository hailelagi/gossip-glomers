package main

import (
	"sync"
)

type replicatedLog struct {
	index map[string]float64
	log   []any
	sync.RWMutex
}

// Commiting k/v data to the log and returns the last index offset
func (l *replicatedLog) Append(key, value any) int {
	l.index[key.(string)] = value.(float64)

	event := []any{key, value}
	l.log = append(l.log, event)

	return len(l.log) - 1
}

// Commiting k/v data to the log and returns the last index offset
/*
func (l *replicatedLog) Read(offsets map[string]int) [][]int {
	for key, offset := range offsets {

	}
	return nil
}
*/

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
