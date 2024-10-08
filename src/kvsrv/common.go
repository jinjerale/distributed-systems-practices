package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// unique identifier for the request
	RequestId int64
	PrevRequestId int64 // Append RequestId seen by the server, -1 means no previous request
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64
	PrevRequestId int64 // RequestId seen by the server
}

type GetReply struct {
	Value string
}
