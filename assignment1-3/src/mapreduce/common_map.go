package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	content, err := ioutil.ReadFile(inFile)
	checkError(err)

	res := mapF(inFile, string(content))

	results := make([][]KeyValue, nReduce)
	for _, v := range res {
		var idx int
		idx = int(ihash(v.Key)) % nReduce
		results[idx] = append(results[idx], v)
	}

	for i := 0; i < nReduce; i++ {
		f, err := os.OpenFile(reduceName(jobName, mapTaskNumber, i), os.O_WRONLY|os.O_CREATE, 0644)
		checkError(err)
		defer f.Close()

		enc := json.NewEncoder(f)
		for _, kv := range results[i] {
			err := enc.Encode(&kv)
			checkError(err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
