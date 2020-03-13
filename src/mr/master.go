package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TODO:
// 1. Make master concurrent [X]
// 2. handle case when all map tasks have been assigned but some is not
// finished. Then should ask idle worker to wait. [X]
// 3. Master notice if a worker has not finished task in 10 seconds. [X]
// 4. Make the worker thread quit when all tasks are done [X]
// 5. Fail to pass "reduce parallism" test. [X]

type mapTaskState struct {
	// It is awkward to use map with indexes. So we make the value of map
	// of type mapTaskState, which contains a "Index" field, to keep track of
	// the index of a input file.
	// The order of calling "range" on a map is undefined!
	Index  int
	Status int
	// Use int to keep track of status of each task:
	// 0: not started and not assigned
	// 1: assigned but not yet finished
	// 2: finished
}

type Master struct {
	// The master needs some data structure to keep track of the states of
	// each map task and each reduce task
	// Read only fields:
	m int // number of map tasks
	r int // number of reduce tasks
	// Mutable fields:
	mapTasks      map[string]mapTaskState // keeping track of map tasks
	mapStartTimes map[string](time.Time)
	mapsDone      bool // whether map tasks all finished

	reduceTasks      []int // keeping track of reduce tasks
	reduceStartTimes [](time.Time)
	reducesDone      bool // whether reduce tasks all finished

	// Mutex for mutable fields
	mutex sync.Mutex // Mutex for concurrent access
}

func (m *Master) GetTask(args *Args, reply *Reply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Try assign ONLY map task when m.mapsDone == false
	if !m.mapsDone {
		for file, state := range m.mapTasks {
			// Two conditions for a task to be eligible for assignment
			// 1. Unassigned
			// 2. Assigned but not finished within 10 seconds
			assignCond1 := state.Status == 0 // unassigned task
			timeout := m.mapStartTimes[file].Add(10 * time.Second).Before(time.Now())
			assignCond2 := state.Status == 1 && timeout
			if assignCond1 || assignCond2 {
				// Construct reply
				reply.Index = state.Index
				reply.TaskType = 0
				reply.InputFile = file
				reply.R = m.r
				reply.M = m.m // Not used

				// update information of the task
				// https://stackoverflow.com/q/42716852/9057530
				// 1. update status
				state.Status = 1 // "assigned and unfinished"
				m.mapTasks[file] = state
				// 2. update start time
				m.mapStartTimes[file] = time.Now()
				return nil
			}
		}

		// Code reach here when:
		// all tasks have been assigned (can be finished or unfinished)
		return errors.New("Map in progress, but all assigned")
	}

	// Try assign reduce task, when m.mapsDone == true && m.reducesDone == false
	if m.mapsDone && !m.reducesDone { // Assign reduce task
		for index, state := range m.reduceTasks {
			// Two conditions for a task to be eligible for assignment
			// 1. Unassigned
			// 2. Assigned but not finished within 10 seconds
			assignCond1 := state == 0 // unassigned task
			timeout := m.reduceStartTimes[index].Add(10 * time.Second).Before(time.Now())
			assignCond2 := state == 1 && timeout
			if assignCond1 || assignCond2 {
				// Construct reply
				reply.Index = index
				reply.TaskType = 1
				reply.InputFile = "" // Not used
				reply.R = m.r
				reply.M = m.m // Not used

				// update information of the task
				// https://stackoverflow.com/q/42716852/9057530
				// 1. update status
				m.reduceTasks[index] = 1 // "assigned but not finished"
				// 2. update start time
				m.reduceStartTimes[index] = time.Now()
				return nil
			}
		}

		return errors.New("Reduce in progress, but all assigned")
	}

	if m.mapsDone && m.reducesDone {
		reply.TaskType = 2 // exit
		return nil
	}

	return errors.New("Somthing wrong")
}

func (m *Master) TaskDone(args *Args, reply *Reply) error {
	// Base case
	if args.TaskType == 0 && m.mapsDone {
		return nil
	}
	if args.TaskType == 1 && m.reducesDone {
		return nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if args.TaskType == 0 && !m.mapsDone { // A map task is done
		for file, state := range m.mapTasks {
			if state.Index == args.Index && state.Status == 1 { // Only when first to finish
				// Update m.mapTasks
				state.Status = 2 // "finished"
				m.mapTasks[file] = state

				// Renaming all temporary files of the map task
				for i := 0; i < m.r; i++ {
					foundFileNames, err := filepath.Glob(fmt.Sprintf("mr-%v-%v-*", args.Index, i))
					if err != nil {
						log.Fatal("filepath.Glob error")
					}

					var chosenFileName string
					if len(foundFileNames) == 1 {
						chosenFileName = foundFileNames[0]
					} else {
						// Choose the longest file, which must have been finished writing
						var maxLen int64 = 0
						for _, fileName := range foundFileNames {
							file, err := os.Open(fileName)
							if err != nil {
								log.Printf("err of Open")
								continue
							}

							fileInfo, err := file.Stat()
							if err != nil {
								log.Printf("err of Stat")
								continue
							}

							if fileInfo.Size() > maxLen {
								maxLen = fileInfo.Size()
								chosenFileName = fileName
							}
							file.Close()
						}
					}
					os.Rename(chosenFileName, fmt.Sprintf("mr-%v-%v", args.Index, i))
				}
				break
			}
		}
		// log.Printf("Map task %v done!\n", args.Index)

		// Possible update to m.mapsDone
		for _, state := range m.mapTasks {
			if state.Status != 2 {
				return nil
			}
		}
		m.mapsDone = true
		log.Println("All map tasks done!")
	} else if args.TaskType == 1 && !m.reducesDone { // A reduce task is done
		m.reduceTasks[args.Index] = 2

		// log.Printf("Reduce task %v done!\n", args.Index)

		// Possible update to m.mapsDone
		for _, state := range m.reduceTasks {
			if state != 2 {
				return nil
			}
		}
		m.reducesDone = true
		log.Println("All reduce tasks done!")
		log.Println("MapReduce done!")
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// https://stackoverflow.com/a/33889389/9057530
	// http.Serve already provides concurrent execution!
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.mapsDone && m.reducesDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.Printf("len(files) = %v, nReduce = %v\n", len(files), nReduce)

	// build a map for map tasks
	mapTasks := make(map[string]mapTaskState)
	for i, file := range files {
		mapTasks[file] = mapTaskState{i, 0}
	}

	// build a map for map tasks starting times
	mapStartTimes := make(map[string](time.Time)) // Does not care about zero value

	// initialize reduceTasks
	reduceTasks := make([]int, nReduce) // Initialized with 0s

	// initialize reduceStartTimes
	reduceStartTimes := make([](time.Time), nReduce) // Does not care about zero value

	// Make a master
	m := Master{m: len(files), r: nReduce,
		mapTasks: mapTasks, mapStartTimes: mapStartTimes, mapsDone: false,
		reduceTasks: reduceTasks, reduceStartTimes: reduceStartTimes, reducesDone: false}

	m.server()
	return &m
}

///////////////////// Example Code //////////////////////////////////////

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }
