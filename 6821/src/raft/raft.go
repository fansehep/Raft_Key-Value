package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
const (
	Master    = 1
	Follower  = 2
	Candidate = 3
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	job         int   //* 服务器当前角色，Master or Follower
	term        int   //* term 当前集群的任期
	lastreserve int64 //* uint64 -> time.Now().UnixMicro()
	voteforid   int   //* 当前对应的 master
	Log         int   //* 表示当前的日志条数
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.term
	var isleader bool = (rf.job == Master)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Cur_term int //* 发起投票的任期
	Cur_log  int //* 发起者的日志条数
	Id       int //* 发起者的 ID
}

type RequestVoteReply struct {
	Ok bool //* true 则代表 我投票给发起投票的人 false 则代表不投票
}

func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//* 投票的结果必须是，我的任期 <= 你的
	//* 且我的日志条目是 <= 你的
	if rf.term >= args.Cur_term {
		reply.Ok = false
		return false
	}
	if rf.Log >= args.Cur_log {
		reply.Ok = false
		return false
	}
	reply.Ok = true
	return true
}

type AppendEntriesArgs struct {
	Heartbeatime int64 //* 更新心跳时间，在 rpc 调用方更新
	Cur_term     int   //* 发起心跳的任期
	Id           int   //* 发起心跳的id
}

type AppendEntriesReply struct {
}

//* 只有当前集群的 master 才会定期向其他的follower 发送心跳
//*
func (rf *Raft) HeartBeatToClient(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[i].Call("Raft.HeartBeat", args, reply)
	return ok
}

//* 心跳也需要判断任期，如果发起者的任期 < 当前的任期
//* 则返回error,
//* raft server中保存了 当前集群的 master id
//* 如果不同，心跳失败
func (rf *Raft) HeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if args.Cur_term < rf.term {
		return false
	}
	if args.Id != rf.voteforid && rf.voteforid != -1 {
		return false
	}
	rf.lastreserve = args.Heartbeatime
	rf.voteforid = args.Id
	return true
}

func (rf *Raft) RaftServerInit() {
	rand.Seed(time.Now().UnixMicro())
	for {
		//* 先 sleep 一段时间，再去判断是否超时
		//* 超时 => 发起投票
		//* 150ms ~ 300ms
		election := rand.Intn(150) + 150
		time.Sleep(time.Duration(election))

		if time.Now().UnixMicro()-rf.lastreserve > (int64)(election+1000) {
			//* 发现超时， 则开始投票
			if rf.job != Master {
				//* 发起选举时，自己给也自己投票
				var vote_me int32 = 1
				args := RequestVoteArgs{}
				args.Cur_log = rf.Log
				//* 发起选举时，自己的 term + 1
				rf.term++
				args.Cur_term = rf.term
				args.Id = rf.me
				for i := range rf.peers {
					go func(servernumber int) {
						reply := RequestVoteReply{}
						err := rf.SendRequestVote(servernumber, &args, &reply)
						if err == false {
							fmt.Printf("error, can not sendRequestVote\n")
						}
						if reply.Ok == true {
							//* 原子相加
							atomic.AddInt32(&vote_me, 1)
						}
					}(i)
					//* 如果发现自己的票数 > 集群的总节点数 / 2
					if vote_me > (int32)(len(rf.peers)/2) {
						//* 自己成为 Master
						//* 并且开始与其他节点发送心跳，
						rf.job = Master
						fmt.Printf("%d be writer \n", rf.me)
					}
				}
			}
		}

		//* 如果自己是 Master, 则给其他节点发送心跳
		if rf.job == Master {
			entryargs := AppendEntriesArgs{}
			entryargs.Cur_term = rf.term
			entryargs.Heartbeatime = time.Now().UnixMicro()
			entryargs.Id = rf.me
			entryreply := AppendEntriesReply{}
			for i := range rf.peers {
				go func(servernumber int) {
					err := rf.HeartBeatToClient(i, &entryargs, &entryreply)
					if err == false {
						fmt.Printf("error, %d hearbeat error!\n", rf.me)
					}
				}(i)
			}
		}

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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.RaftServerInit()

	}
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
//* 有一个服务用于启动服务器
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//* 集群开始的时候都是 follower, 任期为 0,
	rf.job = Follower
	rf.term = 0
	rf.voteforid = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
