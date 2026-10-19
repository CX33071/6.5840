package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"time"
)


type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	getargs:=rpc.GetArgs{Key: key}
	for {//遇到其他错误的时候重试，所以if ok之后没有else，我们不返回，只循环重试
		getreply:=rpc.GetReply{}//放进循环，不然会复用上一次留下的数据
		ok:=ck.clnt.Call(ck.server,"KVServer.Get",&getargs,&getreply)
	if ok {
		if getreply.Err==rpc.ErrNoKey {
			return "",0,rpc.ErrNoKey
		}else if getreply.Err==rpc.OK{
			return getreply.Value,getreply.Version,rpc.OK
		}
	}else{
		time.Sleep(100 * time.Millisecond)
			continue
	}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	putargs:=rpc.PutArgs{Key: key,Value: value,Version: version}
	retried := false
	for {
		putreply:=rpc.PutReply{}
		ok:=ck.clnt.Call(ck.server,"KVServer.Put",&putargs,&putreply)
		if !ok {
            retried = true
            time.Sleep(100 * time.Millisecond)
            continue
        }

        if retried && putreply.Err == rpc.ErrVersion {
            return rpc.ErrMaybe
        }

        return putreply.Err
	}
}
