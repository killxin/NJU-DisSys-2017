package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OP string
	Key string
	Value string
	Cid 	int64
	Index 	int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//db		Database
	//w4c 	Wait4Commit
	db 		map[string]string
	w4c 	map[int]chan Op
	c2i		map[int64]int
}

func (kv *RaftKV) isDuplicate(cid int64,index int) bool{
	if i,ok := kv.c2i[cid];ok{
		return index <= i
	}
	return false
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_,isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
	} else {
		reply.WrongLeader = false
		ch := make(chan Op)
		op := Op{OP: "Get", Key: args.Key, Cid: args.Cid, Index: args.Index}
		index, _, _ := kv.rf.Start(op)
		kv.mu.Lock()
		kv.w4c[index] = ch
		kv.mu.Unlock()
		select {
		case t := <-ch:
			if t == op {
				reply.Err = ""
				kv.mu.Lock()
				reply.Value = kv.db[args.Key]
				kv.mu.Unlock()
			} else {
				reply.Err = Err("fail to get：" + args.Key)
			}
		case <-time.After(10 * time.Second):
			reply.Err = Err("time out：" + args.Key)
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
	} else {
		reply.WrongLeader = false
		ch := make(chan Op)
		op := Op{OP: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, Index: args.Index}
		index, _, _ := kv.rf.Start(op)
		kv.mu.Lock()
		kv.w4c[index] = ch
		kv.mu.Unlock()
		select {
		case t := <-ch:
			if t == op {
				reply.Err = ""
			} else {
				reply.Err = Err("loss：" + args.Key + args.Value)
			}
		case <-time.After(10 * time.Second):
			reply.Err = Err("time out：" + args.Key + args.Value)
		}
	}
}

func (kv *RaftKV) mainLoop(){
	for {
		msg := <- kv.applyCh
		kv.mu.Lock()
		op := msg.Command.(Op)
		if kv.isDuplicate(op.Cid,op.Index) {
			DPrintf("%s <%s,%s> is duplicate RPC\n",op.OP,op.Key,op.Value)
		} else {
			if op.OP == "Put" {
				kv.db[op.Key] = op.Value
			} else if op.OP == "Append" {
				kv.db[op.Key] += op.Value
			}
			kv.c2i[op.Cid] = op.Index
		}
		if ch,ok:=kv.w4c[msg.Index];ok {
			ch <- op
			delete(kv.w4c,msg.Index)
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.w4c = make(map[int]chan Op)
	kv.c2i = make(map[int64]int)
	go kv.mainLoop()

	return kv
}
