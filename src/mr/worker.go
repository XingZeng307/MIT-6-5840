package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := AskArgs{NodePID: os.Getpid()}
		replies := Reply{NodePID: os.Getpid()}
		//log.Printf("%d Worker asks for a task...", os.Getpid())
		call("Coordinator.AskForTask", &args, &replies)
		if replies.TaskType == "Map" {
			//log.Printf("%d Worker received a map task... %d for filename %s", os.Getpid(), replies.TaskIndex, replies.InputFiles[0])
			mapFun(mapf, replies.InputFiles[0], replies.NReducer, replies.TaskIndex)
			//replies.Task.Duration = time.Now().Unix() - replies.Task.Duration
			call("Coordinator.ReportCompletion", &replies, &replies)
			//doMap(&replies, mapf)
		} else if replies.TaskType == "Reduce" {
			//log.Printf("%d Worker received a reduce task... %d", os.Getpid(), replies.TaskIndex)
			reduceFun(reducef, replies.InputFiles, replies.OutputFiles, replies.TaskIndex)
			//replies.Task.Duration = time.Now().Unix() - replies.Task.Duration
			call("Coordinator.ReportCompletion", &replies, &replies)
			//doReduce(&replies, reducef)
		} else if replies.TaskType == "Exit" {
			//log.Printf("%d Worker received Exit", os.Getpid())
			os.Exit(0)
		} else {
			//log.Printf("%d Worker received others.., so wait for 10 seconds", os.Getpid())
			time.Sleep(time.Second)
		}
	}
}

func mapFun(mapf func(string, string) []KeyValue, inputFile string, nReducer int, taskIdx int) {
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %v", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}
	file.Close()

	kva := mapf(inputFile, string(content))
	intermediate := make([][]KeyValue, nReducer)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReducer
		intermediate[r] = append(intermediate[r], kv)
	}

	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", taskIdx, r)
		ofile, _ := ioutil.TempFile("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
}

func reduceFun(reducef func(string, []string) string, inputs []string, outputs []string, reduceBucket int) {
	intermediate := []KeyValue{}
	for _, intermediatefn := range inputs {
		file, err := os.Open(intermediatefn)
		if err != nil {
			log.Fatalf("cannot open %v", intermediatefn)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		if err != nil {
			log.Fatalf("cannot decode intermediate file: %v", err)
		}
	}

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-idx.

	sort.Sort(ByKey(intermediate))

	oname := outputs[0] //mr-out-idx
	ofile, err := ioutil.TempFile("", oname)
	if err != nil {
		log.Fatalf("cannot create temporary output file: %v", err)
	}
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// rename
	os.Rename(ofile.Name(), oname)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
