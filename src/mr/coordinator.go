package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TimeoutThre = 10 * time.Second

type MapReduceTask struct {
	TaskIndex   int
	Status      int // -1 = unassigned, 0 = processing, 1 = compeleted
	Type        string
	InputFiles  []string
	OutputFiles []string
}
type Coordinator struct {
	mapTasks    []MapReduceTask
	reduceTasks []MapReduceTask
	mapDone     bool
	reduceDone  bool
	lock        sync.Mutex
	workerNodes map[int]WorkerNode
}

func (c *Coordinator) ReportCompletion(args *Reply, reply *Reply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	//args.Task.Duration = time.Now().Unix() - args.Task.Duration
	//fmt.Printf("Worker Report Task Index: %d, Worker Report Task Type", args.TaskIndex, args.TaskType)
	if args.TaskType == "Map" {
		c.mapTasks[args.TaskIndex].Status = 1
		delete(c.workerNodes, args.NodePID)
		//fmt.Printf("Worker Report the map %d task has been done", args.TaskIndex)
	} else if args.TaskType == "Reduce" {
		c.reduceTasks[args.TaskIndex].Status = 1
		delete(c.workerNodes, args.NodePID)
		//fmt.Printf("Worker Report the reduce %d task has been done", args.TaskIndex)
	}
	return nil
}

func (c *Coordinator) AskForTask(args *AskArgs, reply *Reply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for pid, worker := range c.workerNodes {
		// Check if the worker has timed out
		if time.Now().Sub(worker.Timestamp) > TimeoutThre {
			//log.Printf("%d TimeoutThre Meeted", pid, worker)
			if worker.AssignedTaskType == "Map" {
				c.mapTasks[worker.AssignedTaskIndex].Status = -1
				//log.Printf("%d TimeoutThre Meeted: Map tasks  %d reset ", pid, worker.AssignedTaskIndex)

			} else if worker.AssignedTaskType == "Reduce" && c.reduceTasks[worker.AssignedTaskIndex].Status != 1 {
				c.reduceTasks[worker.AssignedTaskIndex].Status = -1
				//log.Printf("%d TimeoutThre Meeted: Reduce tasks  %d reset ", pid, worker.AssignedTaskIndex)
			}
			delete(c.workerNodes, pid)
		}
	}

	c.mapDone = true

	indicesMap := make([]int, len(c.mapTasks))
	for i := range indicesMap {
		indicesMap[i] = i
	}

	shuffleIndices(indicesMap)

	for _, idx := range indicesMap {
		mapTask := c.mapTasks[idx]
		if mapTask.Status == -1 {
			c.mapDone = false
			c.mapTasks[idx].Status = 0
			reply.InputFiles = mapTask.InputFiles
			reply.OutputFiles = mapTask.OutputFiles
			reply.TaskIndex = mapTask.TaskIndex
			reply.TaskType = "Map"
			reply.NReducer = len(c.reduceTasks)

			c.workerNodes[args.NodePID] = WorkerNode{
				AssignedTaskIndex: mapTask.TaskIndex,
				AssignedTaskType:  "Map",
				Timestamp:         time.Now(),
			}
			//log.Printf("Register workerNodes PID: %d in Map Task: %d", args.NodePID, idx)
			return nil
		} else if mapTask.Status == 0 {
			c.mapDone = false
			reply.TaskType = "Wait"
			return nil
		}
	}
	c.reduceDone = true
	if c.mapDone {
		indicesReduce := make([]int, len(c.reduceTasks))
		for i := range indicesReduce {
			indicesReduce[i] = i
		}

		// Shuffle the indices slice
		shuffleIndices(indicesReduce)

		for _, idx := range indicesReduce {
			// process reduce tasks
			reduceTask := c.reduceTasks[idx]
			if reduceTask.Status == -1 {
				c.reduceDone = false
				c.reduceTasks[idx].Status = 0
				reply.InputFiles = reduceTask.InputFiles
				reply.OutputFiles = reduceTask.OutputFiles
				reply.TaskIndex = reduceTask.TaskIndex
				reply.TaskType = "Reduce"
				reply.NReducer = len(c.reduceTasks)
				c.workerNodes[args.NodePID] = WorkerNode{
					AssignedTaskIndex: reduceTask.TaskIndex,
					AssignedTaskType:  "Reduce",
					Timestamp:         time.Now(),
				}
				//log.Printf("Register workerNodes PID: %d in Reduce Task: %d", args.NodePID, idx)
				return nil
			} else if reduceTask.Status == 0 {
				c.reduceDone = false
				reply.TaskType = "Wait"
				return nil
			}
		}
	}
	if c.mapDone && c.reduceDone {
		reply.TaskType = "Exit"
	}
	return nil
}

func shuffleIndices(indices []int) {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Shuffle the slice using Fisher-Yates algorithm
	for i := len(indices) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		indices[i], indices[j] = indices[j], indices[i]
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.mapDone && c.reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]MapReduceTask, len(files)),
		reduceTasks: make([]MapReduceTask, nReduce),
		mapDone:     false,
		reduceDone:  false,
		lock:        sync.Mutex{},
		workerNodes: make(map[int]WorkerNode),
	}

	for i := range c.mapTasks {
		c.mapTasks[i] = MapReduceTask{
			TaskIndex:   i,
			Status:      -1,
			Type:        "Map",
			InputFiles:  []string{files[i]},
			OutputFiles: nil,
		}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = MapReduceTask{
			TaskIndex:   i,
			Status:      -1,
			Type:        "Reduce",
			InputFiles:  GenerateInterMediateFiles(i, len(files)),
			OutputFiles: []string{fmt.Sprintf("mr-out-%d", i)},
		}
	}
	c.server()
	return &c
}

func GenerateInterMediateFiles(reduceIndex int, fileCount int) []string {
	var inputFiles []string

	for i := 0; i < fileCount; i++ {
		inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", i, reduceIndex))
	}

	return inputFiles
}
