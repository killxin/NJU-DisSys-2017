package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
	"time"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu			sync.Mutex
	id 			int64
	index		int
	lastLeader	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.lastLeader = 0
	ck.index = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	value := ""
	for i:=ck.lastLeader;true;i=(i+1)%len(ck.servers) {
		args := &GetArgs{Key:key,Cid:ck.id,Index:ck.index}
		reply := &GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get",args,reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = i
			if reply.Err != "" {
				DPrintf("Get from %d with error: %s\n", i, reply.Err)
			} else {
				value = reply.Value
				DPrintf("Get: <%s %s> \n",key,value)
				ck.index++
				break
			}
		} else {
			DPrintf("%d Get <%s,%s,%d> time out from %d\n",ck.id,key,value,ck.index, i)
		}
		<- time.After(10 * time.Millisecond)
	}
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	//DPrintf("%s <%s,%s>\n",op, key, value)
	for i := ck.lastLeader; true; i=(i+1)%len(ck.servers) {
		args := &PutAppendArgs{Key: key, Value: value, Op: op, Cid:ck.id,Index:ck.index}
		reply := &PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = i
			if reply.Err != "" {
				DPrintf("%d %s <%s,%s,%d> to %d with error: %s\n",ck.id,op,key,value,ck.index, i,reply.Err)
			} else {
				DPrintf("%d %s <%s,%s,%d> success\n",ck.id,op,key,value,ck.index)
				ck.index++
				break
			}
		} else {
			DPrintf("%d %s <%s,%s,%d> time out from %d\n",ck.id,op,key,value,ck.index, i)
		}
		<- time.After(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
