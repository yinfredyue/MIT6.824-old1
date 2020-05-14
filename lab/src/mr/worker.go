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

//
// Map functions return a slice of KeyValue.
//
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		taskReply, err := requestTask()
		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// Task assigned
		if taskReply.TaskType == 0 { // map task
			mapTaskNumber := taskReply.Index
			inputFile := taskReply.InputFile
			nReduce := taskReply.R

			kva := runMap(mapf, inputFile)
			writeIntermediate(kva, mapTaskNumber, nReduce)
			notifyTaskDone(0, mapTaskNumber)

		} else if taskReply.TaskType == 1 { // reduce task
			reduceTaskNumber := taskReply.Index
			nMap := taskReply.M

			intermediate := collectIntermediate(reduceTaskNumber, nMap)
			runReduce(reducef, intermediate, reduceTaskNumber)
			notifyTaskDone(1, reduceTaskNumber)
		} else if taskReply.TaskType == 2 { // exit task
			return
		}
	}
}

func requestTask() (Reply, error) {
	args := Args{-1, -1}
	reply := Reply{}
	if call("Master.GetTask", &args, &reply) {
		// log.Printf("Task received: taskType: %v, index: %v, inputFile: %v\n", reply.TaskType, reply.Index, reply.InputFile)
		return reply, nil
	}

	return reply, errors.New("No task")
}

// For map task
func runMap(mapf func(string, string) []KeyValue, fileName string) []KeyValue {
	// Borrowed from mrsequential.go
	// open file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	// read file
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	// Call Map
	return mapf(fileName, string(content))
}

// For map task
func writeIntermediate(kva []KeyValue, mapTaskNumber int, nReduce int) {
	// Split result into multiple reduce tasks

	// Pre-create temporary files
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Cannot find pwd. Error: %v\n", err)
	}
	tempFiles := make([](*os.File), nReduce)
	for i := 0; i < nReduce; i++ {
		tempInterFileName := fmt.Sprintf("mr-%v-%v-*", mapTaskNumber, i) // include "*"
		tempInterFile, err := ioutil.TempFile(pwd, tempInterFileName)
		if err != nil {
			log.Fatalf("Fail to create temporary file %v\n", tempInterFileName)
		}
		tempFiles[i] = tempInterFile
	}

	for _, kv := range kva {
		// Determine reduce task number
		reduceTaskNubmer := ihash(kv.Key) % nReduce

		// Write kv pair to intermediate file
		enc := json.NewEncoder(tempFiles[reduceTaskNubmer])
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Fail to write key-value to file %v\n", tempFiles[reduceTaskNubmer].Name())
		}
	}

	// Close all temp files
	for _, f := range tempFiles {
		f.Close()
	}

	// Renmaing temp files is done by master thread
}

// For reduce task
func collectIntermediate(reduceTaskNubmer int, nMap int) []KeyValue {
	// The returned []KayValue is not sorted
	kva := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", i, reduceTaskNubmer)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("collectIntermediate: Fail to open file %v, %v...\n", fileName, err)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close() // Close file, otherwise "too many open files" error
	}

	return kva
}

// For reduce task
func runReduce(reducef func(string, []string) string, intermediate []KeyValue, reduceTaskNubmer int) {
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reduceTaskNubmer)
	ofile, err := os.Create(oname)

	// Never skip any one error checking/handling!!
	if err != nil {
		log.Fatalf("Cannot create %v\n", oname)
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		// Keeps incrementing j until intermediate[j].key != intermediate[i].key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// values contains all values of key: intermediate[i].Key
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// Run reduce on keys in values
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func notifyTaskDone(taskType int, taskNumber int) error {
	// The worker does not have to notify the master about the created files.
	// Since we have some known format of storing intermedidate files.
	args := Args{taskType, taskNumber}
	reply := Reply{}
	if call("Master.TaskDone", &args, &reply) {
		return nil
	}
	return errors.New("Fail to notify master")
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

///////////////////// Example Code //////////////////////////////////////

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {
// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }
