package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	lockname string
	ownerID string
	ck kvtest.IKVClerk
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck,lockname:lockname,ownerID:kvtest.RandValue(8),}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value,version,err:=lk.ck.Get(lk.lockname)
	if err== rpc.ErrNoKey{
		err1:=lk.ck.Put(lk.lockname,"",0)
		if err1==rpc.OK{
		}else{
			time.Sleep(10 * time.Millisecond)
		}
		continue
	}else if err==rpc.OK{
		if value==lk.ownerID{
			return
		}
		if value!=""{
			time.Sleep(10 * time.Millisecond)
			continue
		}else{
			err2:=lk.ck.Put(lk.lockname,lk.ownerID,version)
			if err2==rpc.OK{
				return
			}else{
				continue
			}
		}
	}else{
		time.Sleep(10 * time.Millisecond)
		continue
	}
	}

}

func (lk *Lock) Release() {
	for {
		value,version,err:=lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey{
			return
		}else{
		if value == "" {
			return
		}else if value==lk.ownerID{
		err:=lk.ck.Put(lk.lockname,"",version)
		if err == rpc.OK{
			return
		}else{
			time.Sleep(10 * time.Millisecond)
		}
		}else{
			return
		}
		}
	}
}
