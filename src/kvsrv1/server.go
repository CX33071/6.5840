package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	Value string
	Version rpc.Tversion//rpc.go自定义的一个go类型，底层类型是uint64
}
type KVServer struct {
	mu sync.Mutex
	data map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data:make(map[string]ValueVersion),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key:=args.Key
	value,exists:=kv.data[key]
	if exists {
		reply.Value=value.Value
		reply.Version=value.Version
		reply.Err=rpc.OK
	}else {
		reply.Err=rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key:=args.Key
	value,exists:=kv.data[key]
	if !exists{
		if args.Version==0 {
			kv.data[key]=ValueVersion{Value:args.Value,Version: 1}
			reply.Err=rpc.OK
		}else {
			reply.Err=rpc.ErrNoKey
		}
	}else{
		if args.Version!=value.Version{
			reply.Err=rpc.ErrVersion
		}else{
			kv.data[key]=ValueVersion{Value: args.Value,Version: args.Version+1}
			reply.Err=rpc.OK
		}
	}
}



// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
