package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
	"fmt"
)
//import "labrpc"
//import "bytes"
//import "encoding/gob"



//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Server State
const(
	Leader = iota //0
	Follower
	Candidate
)

type LogEntry struct {
	term 		int
	index		int
	command		interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state		int
	// Persistent state on all servers
	currentTerm	int
	voteFor		int
	log			[]LogEntry
	// Volatile state on all servers
	commitIndex	int
	lastApplied	int
	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int
	// Having received AppendEntries on followers
	heartBeat	bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&rf.currentTerm)
	 d.Decode(&rf.voteFor)
	 d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

func (rf *Raft) MakeRequestVoteArgs() RequestVoteArgs{
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	// skip some fields
	return args
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	reply.VoteGranted = rf.voteFor == -1 || rf.voteFor == args.CandidateId // && rf.isUp2Date(args)
	if reply.VoteGranted && rf.voteFor == -1 {
		rf.voteFor = args.CandidateId
	}
}

func (rf *Raft) isUp2Date(args RequestVoteArgs) bool {
	if rf.log[rf.lastApplied].term != args.LastLogTerm {
		return rf.log[rf.lastApplied].term < args.LastLogTerm
	} else {
		return rf.lastApplied <= args.LastLogIndex
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	fmt.Println("Start!!!!!")
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, /*len=*/1, /*cap=*/100)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.heartBeat = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.passiveLoop()
	return rf
}

func RandElectionTimeout() int {
	return rand.Intn(450) + 10
}

func (rf *Raft) passiveLoop(){
	// random election timeout [10,500]ms
	for rf.state != Leader {
		timer := time.NewTimer(time.Duration(RandElectionTimeout()) * time.Millisecond)
 		select {
		case <-timer.C:
			if !rf.heartBeat {
				rf.currentTerm++
				rf.state = Candidate
				ch := make(chan *RequestVoteReply)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.SendRequestVote(i, ch)
					}
				}
				votes := 1 // vote for itself
				for i := 0; i < len(rf.peers)-1; i++ {
					reply := <-ch
					fmt.Println(rf.me,"reply",reply)
					if reply != nil && reply.VoteGranted {
						votes++
					}
				}
				fmt.Println(rf.me,"votes",votes)
				if votes >= len(rf.peers)/2 + 1 {
					rf.state = Leader
					fmt.Println("Term",rf.currentTerm,"Leader Change ",rf.me)
				}
			}
			rf.heartBeat = false
		}
	}
	go rf.activeLoop()
}

func (rf *Raft) activeLoop(){
	for rf.state == Leader {
		ch := make(chan *AppendEntriesReply)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.SendHeartBeat(i, ch)
			}
		}
		for i := 0; i < len(rf.peers)-1; i++ {
			<-ch
		}
	}
	// never execute
	//go rf.passiveLoop()
}

func (rf *Raft) SendRequestVote(i int,ch chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(i, rf.MakeRequestVoteArgs(), reply){
		ch <- reply
	} else {
		ch <- nil
	}
}

func (rf *Raft) SendHeartBeat(i int,ch chan *AppendEntriesReply) {
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(i, rf.MakeHeartBeat(), reply) {
		ch <- reply
	} else {
		ch <- nil
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	//Entries      []LogEntry bug when encoding
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Println("yyy",args.Term)
	// just deal with heart beat
	if args.Term < rf.currentTerm || rf.state == Leader {
		return
	}
	if rf.state == Candidate {
		rf.state = Follower
	}
	rf.heartBeat = true
	rf.currentTerm = args.Term
}

func (rf *Raft) MakeHeartBeat() AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	// skip some fields
	return args
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("xxx",args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


