package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"encoding/json"
	"os"
	"bytes"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	data, err := ioutil.ReadFile(inFile)
	if err != nil {
		panic(err)
	}
	var keyvalues []KeyValue
	keyvalues = mapF("", string(data))
	intermediateFiles := make([]string, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	buffers := make([]bytes.Buffer, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediateFiles[i] = reduceName(jobName, mapTask, i)
		encoders[i] = json.NewEncoder(&buffers[i])
	}
	for i := 0; i < len(keyvalues); i++ {
		idx := ihash(keyvalues[i].Key)%nReduce
		encoders[idx].Encode(keyvalues[i])
	}
	for i := 0; i < nReduce; i++ {
		f,_ := os.Create(intermediateFiles[i])
		f.Write(buffers[i].Bytes())
		f.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
