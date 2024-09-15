package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "io/ioutil"
import "sort"
import "time"
import "path/filepath"


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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// keep calling rpc until all tasks are done
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		connected := call("Coordinator.GetTask", &args, &reply)
		if !connected || reply.TaskType == NoTask {
			break
		} else if reply.TaskType == WaitTask || reply.TaskNum == -1 {
			// wait a while
			log.Printf("No task available, waiting for 5 seconds")
			time.Sleep(5 * time.Second)
			continue
		} else if reply.TaskType == MapTask {
			HandleMapTask(mapf, &reply)
		} else if reply.TaskType == ReduceTask {
			HandleReduceTask(reducef, &reply)
		}
		log.Printf("Task %d done", reply.TaskNum)

		// inform the coordinator that the task is done
		taskDoneArgs := TaskDoneArgs{TaskType: reply.TaskType, TaskNum: reply.TaskNum}
		taskDoneReply := TaskDoneReply{}
		call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	}
}

func HandleMapTask(mapf func(string, string) []KeyValue, reply *GetTaskReply) {
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content)) // []KeyValue
	sort.Sort(ByKey(intermediate))
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
		mapTaskNum := reply.TaskNum
		reduceTaskNum := ihash(intermediate[i].Key) % reply.NReduce
		oname := fmt.Sprintf("mr-%d-%d", mapTaskNum, reduceTaskNum)
		// need to append the file if it already exists
		// encode the key and values to the file in json format
		ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(ofile)
		for _, value := range values {
			kv := KeyValue{Key: intermediate[i].Key, Value: value}
			e := enc.Encode(&kv)
			if e != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		ofile.Close()
		i = j
	}
}

func HandleReduceTask(reducef func(string, []string) string, reply *GetTaskReply) {
	taskNum := reply.TaskNum
	// grep all files with the pattern "mr-*-%d" % taskNum
	files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", taskNum))
	if err != nil {
		log.Fatalf("cannot find files with pattern mr-*-%d", taskNum)
	}
	intermediate := []KeyValue{}
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", taskNum)
	ofile, _ := os.Create(oname)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
