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

type opInfo struct {
	opID  int64
	value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data    map[string]string
	session map[int64]opInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.session[args.ClientID].opID != args.OpID {
		kv.session[args.ClientID] = opInfo{opID: args.OpID}
		kv.data[args.Key] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.session[args.ClientID].opID != args.OpID {
		reply.Value = kv.data[args.Key]
		kv.session[args.ClientID] = opInfo{opID: args.OpID, value: reply.Value}
		kv.data[args.Key] = kv.data[args.Key] + args.Value
	} else {
		reply.Value = kv.session[args.ClientID].value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.session = make(map[int64]opInfo)

	return kv
}
