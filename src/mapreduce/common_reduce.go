package mapreduce

import (
	"fmt"
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
	fmt.Println("in reduce")
	keyvaluesMap := make(map[string][]string)
	var result []KeyValue
	for i := 0; i < nMap; i++ {
		fmt.Println("reading ", reduceName(jobName, i, reduceTask))
		/*b, err := ioutil.ReadFile(reduceName(jobName, i, reduceTask))
		fmt.Println("size read from file", len(b))
		if err != nil {
			fmt.Println("error", err, "while reading file: ", reduceName(jobName, 0, 1))
			panic(err)
	    }*/
		file,err1 := os.Open(reduceName(jobName, i, reduceTask))
		if err1 != nil {
			fmt.Println("error", err1, "while reading file: ", reduceName(jobName, 0, 1))
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
				fmt.Println("Error ", decErr, " while decoding ctr", ctr)
				panic(decErr)
			}
			ctr++
			keyvaluesMap[kv.Key] = append(keyvaluesMap[kv.Key], kv.Value)
		}
		fmt.Println("number of keyvalues read in reduce ", ctr)
		file.Close()
	}
	for k := range keyvaluesMap {
		//fmt.Println("going to reduce key: ", k, "value: ", keyvaluesMap[k])
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
		//fmt.Println("result key: ", kv.Key, " value: ", kv.Value)
		enc.Encode(kv)
	}
	f.Close()
	/*
	fmt.Println(reduceName(jobName, 0, 1))
	b, err := ioutil.ReadFile(reduceName(jobName, 0, 1))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("length of byte array", len(b))
	var keyvalues []KeyValue
	json.NewDecoder(bytes.NewReader(b)).Decode(&keyvalues)
	fmt.Println("print length of struct")
	fmt.Println(len(keyvalues))
	*/
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
