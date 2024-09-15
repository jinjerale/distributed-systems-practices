package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// unique identifier for the request
	RequestId string
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId string
}

type GetReply struct {
	Value string
}
