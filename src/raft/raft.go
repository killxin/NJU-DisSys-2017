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
	"math"
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
	Term 		int
	Command		interface{}
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
	commitIndex	int // index of highest log entry known to be committed
	lastApplied	int // index of highest log entry applied to state machines
	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int
	// Having received AppendEntries on followers
	heartBeat	bool
	applyCh 	chan ApplyMsg
	// Count votes in Candidate
	votes		int
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
	//DPrintf(rf.me,"presist",rf.currentTerm,rf.voteFor,rf.log)
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
	//DPrintf(rf.me,"readPresist",rf.currentTerm,rf.voteFor,rf.log)
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
	term,isLeader := rf.GetState()
	if isLeader {
		DPrintf("%d receives command %v\n",rf.me, command)
		// add command to log
		rf.mu.Lock()
		entry := LogEntry{term,command}
		rf.log = append(rf.log, entry)
		rf.persist()
		DPrintf("%d log %v\n",rf.me,rf.log)
		index = len(rf.log) - 1
		// issue AppendEntries RPCs to others
		// respond after entry applied to state machine
		rf.mu.Unlock()
	}
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
	rf.log = []LogEntry{LogEntry{0,nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.heartBeat = false
	rf.applyCh = applyCh
	rf.votes = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.followerLoop()
	return rf
}

func RandElectionTimeout() int {
	// RaftElectionTimeout = 1000ms
	return rand.Intn(500) + 500
}

func (rf *Raft) X2Follower(){
	DPrintf("%d becomes follower\n",rf.me)
	rf.state = Follower
	go rf.followerLoop()
}

func (rf *Raft) X2Candidate(){
	DPrintf("%d becomes candidate\n",rf.me)
	rf.state = Candidate
	go rf.candidateLoop()
}

func (rf *Raft) X2Leader(){
	DPrintf("%d becomes leader\n",rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
	}
	go rf.leaderLoop()
}

func (rf *Raft) followerLoop(){
	rf.heartBeat = false
	// random election timeout [500,1000]ms
	for rf.state == Follower {
		timer := time.NewTimer(time.Duration(RandElectionTimeout()) * time.Millisecond)
 		<-timer.C
		if rf.state == Follower && !rf.heartBeat {
			rf.X2Candidate()
			return
		}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			DPrintf("%d applies %v\n", rf.me, rf.log[i].Command)
			rf.applyCh <- ApplyMsg{Index: i, Command: rf.log[i].Command}
			rf.lastApplied = i
		}
		rf.heartBeat = false
	}
}

func (rf *Raft) candidateLoop(){
	for rf.state == Candidate {
		rf.mu.Lock()
		rf.currentTerm++
		// vote for itself
		rf.voteFor = rf.me
		rf.persist()
		rf.votes = 1
		DPrintf("%d start election term %d\n",rf.me,rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.SendRequestVote(i)
			}
		}
		rf.mu.Unlock()
		timer := time.NewTimer(time.Duration(RandElectionTimeout()) * time.Millisecond)
		<-timer.C
	}
}

func (rf *Raft) leaderLoop(){
	for rf.state == Leader {
		args := make([]AppendEntriesArgs, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args[i] = rf.MakeAppendEntriesArgs(i)
				go rf.SendAppendEntries(i, args[i])
			}
		}
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			if rf.log[N].Term == rf.currentTerm {
				count := 0
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}
				if count >= len(rf.peers)/2 {
					DPrintf( "%d commitIndex %d\n", rf.me, N)
					rf.commitIndex = N
					break
				}
			}
		}
		timer := time.NewTimer(time.Duration(200) * time.Millisecond)
		//go func() {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			DPrintf( "%d applies %v\n", rf.me, rf.log[i].Command)
			rf.applyCh <- ApplyMsg{Index: i, Command: rf.log[i].Command}
			rf.lastApplied = i
		}
		//}()
		<-timer.C
	}
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
	From		 int // for debug
	Term        int
	VoteGranted bool
}

func (rf *Raft) MakeRequestVoteArgs() RequestVoteArgs{
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.log) -1
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	args.CandidateId = rf.me
	return args
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//DPrintf(rf.me,"receive request vote",args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if rf.state == Follower {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.voteFor = -1
			if rf.isUp2Date(args) {
				rf.voteFor = args.CandidateId
				// grant votes to candidate will reset heartbeat
				rf.heartBeat = true
				reply.VoteGranted = true
			}
		} else if args.Term == rf.currentTerm {
			if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isUp2Date(args) {
				rf.voteFor = args.CandidateId
				// grant votes to candidate will reset heartbeat
				rf.heartBeat = true
				reply.VoteGranted = true
			}
		}
	} else if rf.state == Candidate {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.voteFor = -1
			DPrintf("%d candidate get reqvot %v\n",args)
			rf.X2Follower()
			if rf.isUp2Date(args) {
				rf.voteFor = args.CandidateId
				reply.VoteGranted = true
			}
		}
	} else if rf.state == Leader {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.voteFor = -1
			DPrintf("%d leader get reqvot %v\n", rf.me, args)
			rf.X2Follower()
			if rf.isUp2Date(args) {
				rf.voteFor = args.CandidateId
				reply.VoteGranted = true
			}
		}
	}
	reply.From = rf.me
	reply.Term = rf.currentTerm
}

func (rf *Raft) isUp2Date(args RequestVoteArgs) bool {
	//DPrintf(rf.me,rf.log,"vs.",args)
	if rf.log[len(rf.log)-1].Term != args.LastLogTerm {
		return rf.log[len(rf.log)-1].Term < args.LastLogTerm
	} else {
		return len(rf.log) - 1 <= args.LastLogIndex
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

func (rf *Raft) SendRequestVote(i int) {
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(i, rf.MakeRequestVoteArgs(), reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			DPrintf("%d get vote reply %v\n", rf.me, reply)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.persist()
				rf.X2Follower()
				return
			} else if reply.VoteGranted {
				rf.votes++
			}
			if rf.votes >= len(rf.peers)/2+1 {
				rf.X2Leader()
				return
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries      []LogEntry //bug when encoding
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	From		int //for debug
	Term		int
	Success		bool
	NextIndex	int //update nextIndex in leader
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// just deal with heart beat
	//DPrintf(rf.me,"receive append entries",args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state == Follower {
		rf.heartBeat = true
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
	} else if rf.state == Candidate {
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			DPrintf("%d candidate get appent %v\n",rf.me,args)
			rf.X2Follower()
		}
	} else if rf.state == Leader {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			DPrintf("%d leader get appent %v\n",rf.me,args)
			rf.X2Follower()
		}
	}
	reply.From = rf.me
	reply.Term = rf.currentTerm
	// deal log replication
	if len(args.Entries)!=0 {
		DPrintf( "%d get appent %v\n",rf.me,args)
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		// leader will become follower then become leader during wait rpc reply
		reply.NextIndex = args.PrevLogIndex + 1
	} else if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		// optimization
		reply.NextIndex = len(rf.log)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.NextIndex = 1
		// optimization
		for i:=args.PrevLogIndex-1;i>=0;i--{
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.NextIndex = i + 1
				break
			}
		}
	} else {
		if len(rf.log) > args.PrevLogIndex+1 {
			rf.log = rf.log[:args.PrevLogIndex+1]
		}
		rf.log = append(rf.log, args.Entries...)
		if len(args.Entries) != 0 {
			DPrintf( "%d log %v\n", rf.me,rf.log)
		}
		reply.Success = true
		reply.NextIndex = len(rf.log)

		if args.LeaderCommit > rf.commitIndex {
			N := int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
			DPrintf( "%d commitIndex %v\n",rf.me, N)
			rf.commitIndex = N
		}
		//go func() {
		//	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		//		DPrintf(rf.me, "applies", rf.log[i].Command)
		//		rf.applyCh <- ApplyMsg{Index: i, Command: rf.log[i].Command}
		//		rf.lastApplied = i
		//	}
		//}()
	}
}

func (rf *Raft) MakeAppendEntriesArgs(i int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	// if len(rf.log) == rf.nextIndex[i] HeartBeat
	args.PrevLogIndex = rf.nextIndex[i] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = rf.log[rf.nextIndex[i]:] // index out of length return []
	args.LeaderCommit = rf.commitIndex
	return args
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntries(i int, args AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Leader {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.persist()
				DPrintf("%d get appentries reply %v\n", rf.me, reply)
				rf.X2Follower()
				return
			} else if reply.Success {
				rf.matchIndex[i] = reply.NextIndex - 1
				if rf.nextIndex[i] < reply.NextIndex {
					DPrintf( "%d increase nextIndex %d in %d\n", i, reply.NextIndex, rf.me)
				}
				rf.nextIndex[i] = reply.NextIndex
			} else {
				rf.nextIndex[i] = reply.NextIndex
				DPrintf("%d decrease nextIndex %d in %d\n", i,rf.nextIndex[i], rf.me)
			}
		}
	}
}
