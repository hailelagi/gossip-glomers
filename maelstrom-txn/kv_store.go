package main

type store struct {
	index map[int]int
	log   []float64
}

func (kv *store) newTxn(txn []any) [][]any {
	var result = make([][]any, 0)

	for _, op := range txn {
		op := op.([]any)

		if op[0] == "r" {
			index := op[1].(float64)

			result = append(result, []any{"r", index, kv.log[int(index)]})
		} else if op[0] == "w" {
			index := op[1].(float64)
			value := op[2].(float64)
			kv.log[int(index)] = value

			result = append(result, []any{"w", index, kv.log[int(index)]})
		}
	}

	return result
}

func (kv *store) syncTxn(txn []any) {
	for _, op := range txn {
		op := op.([]any)

		if op[0] == "r" {
			continue
		} else if op[0] == "w" {
			index := op[1].(float64)
			value := op[2].(float64)
			kv.log[int(index)] = value
		}
	}
}
