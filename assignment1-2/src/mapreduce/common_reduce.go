package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	records := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		f, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		checkError(err)
		defer f.Close()

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			records = append(records, kv)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})

	results := make([]KeyValue, 0)
	for i := 0; i < len(records); {
		var values []string
		key := records[i].Key
		for i < len(records) && key == records[i].Key {
			values = append(values, records[i].Value)
			i++
		}
		reducedRes := reduceF(key, values)
		results = append(results, KeyValue{key, reducedRes})
	}

	f, err := os.OpenFile(mergeName(jobName, reduceTaskNumber), os.O_WRONLY|os.O_CREATE, 0644)
	checkError(err)
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, kv := range results {
		err := enc.Encode(&kv)
		checkError(err)
	}
}
