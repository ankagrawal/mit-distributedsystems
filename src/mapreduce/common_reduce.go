package mapreduce

import (
	"encoding/json"
	"sort"
	"os"
	"io"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	keyvaluesMap := make(map[string][]string)
	var result []KeyValue
	for i := 0; i < nMap; i++ {
		file,err1 := os.Open(reduceName(jobName, i, reduceTask))
		if err1 != nil {
			panic(err1)
	    }
		decoder := json.NewDecoder(file)
		ctr := 0
		for {
			var kv KeyValue
			decErr := decoder.Decode(&kv)
			if decErr == io.EOF {
				break
			} else if decErr != nil {
				panic(decErr)
			}
			ctr++
			keyvaluesMap[kv.Key] = append(keyvaluesMap[kv.Key], kv.Value)
		}
		file.Close()
	}
	for k := range keyvaluesMap {
		resultVal := reduceF(k, keyvaluesMap[k])
		var kv KeyValue
		kv.Key = k
		kv.Value = resultVal
		result = append(result, kv)
	}
	sort.Sort(KeyValueSorter(result))
	f,_ := os.Create(outFile)
	enc := json.NewEncoder(f)
	for _, kv := range result {
		enc.Encode(kv)
	}
	f.Close()
}
