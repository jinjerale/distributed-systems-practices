package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
// import "fmt"

type Coordinator struct {
	// Your definitions here.
	mu        sync.Mutex
	files     []string
	NReduce   int
	midx       int
	ridx	   int
	mapTasks  []bool // tracks completed map tasks
	redTasks  []bool // tracks completed reduce tasks
	done      bool
}

// Your code here -- RPC handlers for the worker to call.


// Assign a task to the worker
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.midx < len(c.files) {
		reply.TaskNum = c.midx
		reply.NReduce = c.NReduce
		reply.Filename = c.files[c.midx]
		reply.TaskType = MapTask
		c.mapTasks[c.midx] = false // task assigned but not completed
		c.midx++
	} else if !c.allMapTasksDone() {
		reply.TaskType = WaitTask
		reply.TaskNum = -1 // signal worker to wait
	} else if !c.allReduceTasksDone() && c.ridx < c.NReduce {
		reply.TaskType = ReduceTask
		reply.TaskNum = c.ridx
		reply.NReduce = c.NReduce
		c.redTasks[c.ridx] = false
		c.ridx++
	} else {
		reply.TaskType = NoTask
		reply.TaskNum = -1 // all tasks completed
	}

	return nil
}

// Mark a task as done
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		// fmt.Printf("Map task %d done\n", args.TaskNum)
		c.mapTasks[args.TaskNum] = true
	} else if args.TaskType == ReduceTask {
		// fmt.Printf("Reduce task %d done\n", args.TaskNum)
		c.redTasks[args.TaskNum] = true
	}
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

// Check if all map tasks are completed
func (c *Coordinator) allMapTasksDone() bool {
	for _, done := range c.mapTasks {
		if !done {
			return false
		}
	}
	return true
}

// Check if all reduce tasks are completed
func (c *Coordinator) allReduceTasksDone() bool {
	for _, done := range c.redTasks {
		if !done {
			return false
		}
	}
	return true
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.allMapTasksDone() && c.allReduceTasksDone() {
		c.done = true
	}
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	lenMapTasks := len(files)
	lenRedTasks := nReduce
	c := Coordinator{
		files:    files,
		NReduce:  nReduce,
		midx:     0,
		ridx:     0,
		mapTasks: make([]bool, lenMapTasks),
		redTasks: make([]bool, lenRedTasks),
		done:     false,
	}

	// Start the server
	c.server()
	return &c
}
