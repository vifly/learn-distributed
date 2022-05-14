package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var RpcCallError = errors.New("Can't call RPC func")
var NotInputFileError = errors.New("There is no file to process")

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply, err := GetWorkInfo()
		if err != nil {
			log.Fatalf(err.Error())
		}
		if reply.IsEnd {
			// fmt.Println("end")
			return
		}

		switch reply.WorkType {
		case Map:
			HandleMapWork(mapf, reply)
		case Reduce:
			HandleReduceWork(reducef, reply)
		default:
			log.Fatalf("Unknown work type")
		}
		time.Sleep(time.Second)
	}

}

func HandleMapWork(mapf func(string, string) []KeyValue, reply GetWorkReply) {
	filePath := reply.FilePath
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}

	kva := mapf(filePath, string(contents))
	intermediates := make([][]KeyValue, reply.NReduce)
	outputFilePaths := make([]string, 0)

	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % reply.NReduce
		intermediates[reduceNum] = append(intermediates[reduceNum], kv)
	}

	for reduceNum, intermediate := range intermediates {
		if len(intermediate) < 1 {
			continue
		}
		outputFile, err := ioutil.TempFile("./", "temp-map-")
		defer outputFile.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		enc := json.NewEncoder(outputFile)
		for _, kv := range intermediate {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf(err.Error())
			}
		}
		os.Rename(outputFile.Name(), getIntermediatePath(reply.TaskId, reduceNum))
		outputFilePaths = append(outputFilePaths, getIntermediatePath(reply.TaskId, reduceNum))
	}

	NotifyWorkDone(reply.TaskId, reply.WorkType, outputFilePaths)
}

func HandleReduceWork(reducef func(string, []string) string, reply GetWorkReply) {
	filePaths := getIntermediatePathsById(reply.TaskId)
	intermediate := make([]KeyValue, 0)
	for _, path := range filePaths {
		file, err := os.Open(path)
		if err != nil {
			log.Fatalf("cannot open %v", path)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := getReduceOutputPath(reply.TaskId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// j 代表在已排序的数组中与 intermediate[i].Key 相同的最后一个元素的下标
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	NotifyWorkDone(reply.TaskId, Reduce, []string{oname})
}

func GetWorkInfo() (GetWorkReply, error) {
	args := NullArgs{}
	reply := GetWorkReply{}

	ok := call("Coordinator.GetWork", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, RpcCallError
	}
}

func NotifyWorkDone(taskId int, workType WorkType, outputFilePaths []string) error {
	args := WorkDoneArgs{taskId, workType, outputFilePaths}
	reply := NullReply{}

	ok := call("Coordinator.WorkDone", &args, &reply)
	if ok {
		return nil
	} else {
		return RpcCallError
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
