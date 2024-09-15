package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// need a map to store key-value pairs
	mp map[string]string
	// need a map to store request id and the value returned
	requestIdMap map[int64]bool
	cache map[int64]string // append cache
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// remove cache entry that has been seen by the server
	if args.PrevRequestId != -1 {
		delete(kv.cache, args.PrevRequestId)
	}

	if val, ok := kv.mp[args.Key]; ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// remove cache entry that has been seen by the server
	if args.PrevRequestId != -1 {
		delete(kv.cache, args.PrevRequestId)
	}

	// check if the request id has been processed\
	if _, ok := kv.requestIdMap[args.RequestId]; ok {
		// log.Printf("request id %s has been processed", args.RequestId)
		// don't care about the value
		return
	}
	kv.mp[args.Key] = args.Value
	reply.Value = args.Value
	kv.requestIdMap[args.RequestId] = true
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// remove cache entry that has been seen by the server
	if args.PrevRequestId != -1 {
		delete(kv.cache, args.PrevRequestId)
	}
	// check if the request id has been processed
	if val, ok := kv.cache[args.RequestId]; ok {
		// log.Printf("request id %s has been processed", args.RequestId)
		reply.Value = val
		return
	}
	if val, ok := kv.mp[args.Key]; ok {
		reply.Value = val
		kv.mp[args.Key] = val + args.Value
		kv.cache[args.RequestId] = val
	} else {
		reply.Value = ""
		kv.mp[args.Key] = args.Value
		kv.cache[args.RequestId] = ""
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mp = make(map[string]string)
	kv.requestIdMap = make(map[int64]bool)
	kv.cache = make(map[int64]string)

	return kv
}
