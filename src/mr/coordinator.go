package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
// import "fmt"

type TaskState int

const (
   TaskPending TaskState = iota
   TaskInProgress
   TaskCompleted
)

type TaskInfo struct {
   state TaskState
   startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu        sync.Mutex
	files     []string
	NReduce   int
	midx       int
	ridx	   int
	mapTasks  []TaskInfo // tracks completed map tasks
	redTasks  []TaskInfo // tracks completed reduce tasks
	done      bool
}

// Your code here -- RPC handlers for the worker to call.
func GetPendingTask(tasks []TaskInfo) int {
	for i, task := range tasks {
		if task.state == TaskPending {
			return i
		}
	}
	return -1
}

// Retrieve in progress task to pending if it has been running for over 10 seconds
func ResetTask(tasks []TaskInfo) {
	for i, task := range tasks {
		if task.state == TaskInProgress && time.Since(task.startTime) > 10 * time.Second {
			tasks[i].state = TaskPending
		}
	}
}

// Assign a task to the worker
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.allMapTasksDone() {
		ResetTask(c.mapTasks)
		idx := GetPendingTask(c.mapTasks)
		if idx != -1 {
			reply.TaskNum = idx
			reply.NReduce = c.NReduce
			reply.Filename = c.files[idx]
			reply.TaskType = MapTask
			c.mapTasks[idx].state = TaskInProgress
			c.mapTasks[idx].startTime = time.Now()
		} else {
			reply.TaskType = WaitTask
			reply.TaskNum = -1 // signal worker to wait
		}
	} else if !c.allReduceTasksDone() {
		ResetTask(c.redTasks)
		idx := GetPendingTask(c.redTasks)
		if idx != -1 {
			reply.TaskType = ReduceTask
			reply.TaskNum = idx
			reply.NReduce = c.NReduce
			c.redTasks[idx].state = TaskInProgress
			c.redTasks[idx].startTime = time.Now()
		} else {
			reply.TaskType = WaitTask
			reply.TaskNum = -1 // signal worker to wait
		}
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
		c.mapTasks[args.TaskNum].state = TaskCompleted
	} else if args.TaskType == ReduceTask {
		// fmt.Printf("Reduce task %d done\n", args.TaskNum)
		c.redTasks[args.TaskNum].state = TaskCompleted
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
	for _, task := range c.mapTasks {
		if task.state != TaskCompleted {
			return false
		}
	}
	return true
}

// Check if all reduce tasks are completed
func (c *Coordinator) allReduceTasksDone() bool {
	for _, task := range c.redTasks {
		if task.state != TaskCompleted {
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
		mapTasks: make([]TaskInfo, lenMapTasks),
		redTasks: make([]TaskInfo, lenRedTasks),
		done:     false,
	}

	// Start the server
	c.server()
	return &c
}
