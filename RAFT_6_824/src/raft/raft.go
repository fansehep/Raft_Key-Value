package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, IsLeaader)
//   start agreement on a new log entry
// rf.GetState() (term, IsLeaader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"log"
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

const (
	Leader    = 1
	Follower  = 2
	Candidate = 3 //* 候选者
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	job         int64          //* 服务器当前角色，Leader or Follower
	term        int64          //* term 当前集群的任期
	lastreserve int64          //* uint64 -> time.Now().UnixMicro()
	Leader_id   int64          //* 当前集群的Leader ID
	Log         []int          //* 表示当前的日志条数
	Vote_map    map[int64]bool //* 每次只会给一个任期中的一次发起选举投票
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int = (int)(rf.term)
	var IsLeaader bool = (rf.job == Leader)
	rf.mu.Unlock()
	return term, IsLeaader
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
	Term        int64 //* Candidate 的任期
	CandidateID int64 //* candidate 的ID

}

type RequestVoteReply struct {
	Term    int64 //* 被调用 rpc 方的 任期
	Success bool  //* ture 为被调用方投，false则反之
}

func (rf *Raft) SendRequestVote(server int32, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//* 一次投票过程中，只能给一个任期中的一名发起者，投一次票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.term
	if args.Term >= rf.term {
		if !rf.Vote_map[args.Term] {
			reply.Success = true
			log.Printf("id: %d term: %d vote %d %d", rf.me, rf.term, args.CandidateID, args.Term)
			rf.Vote_map[args.Term] = true
			rf.mu.Unlock()
			return
		} else {
			reply.Success = false
			log.Printf("id: %d term: %d no vote %d %d", rf.me, rf.term, args.CandidateID, args.Term)
			rf.mu.Unlock()
			return
		} 
	} else {
		reply.Success = false
		log.Printf("id: %d term: %d no vote %d %d", rf.me, rf.term, args.CandidateID, args.Term)
		rf.mu.Unlock()
		return
	}
}

type AppendEntriesArgs struct {
	Term         int64 //* Leader 的任期
	LeaderID     int64 //* Leader ID
	Heartbeatime int64 //* 心跳，用来更新时间
}

type AppendEntriesReply struct {
	Term    int64 //* 回复者的任期
	Sussess bool  //*
}

func (rf *Raft) SendAppendEntries(i int32, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	er := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	return er
}

//* 心跳也需要判断任期，如果发起者的任期 < 当前的任期
//* 则返回error,
//* raft server中保存了 当前集群的 master id
//* 如果不同，心跳失败
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.term {
		reply.Term = atomic.LoadInt64(&rf.term)
		reply.Sussess = false
		rf.mu.Unlock()
		return
	}
	//* 中途受到高 term 的 Leader 心跳
	//* 则变为Follower
	atomic.SwapInt64(&rf.job, Follower)
	atomic.SwapInt64(&rf.lastreserve, args.Heartbeatime)
	atomic.SwapInt64(&rf.term, args.Term)
	reply.Term = rf.term
	log.Printf("%d %d to %d %d append sussess", args.LeaderID, args.Term, rf.me, rf.term)
	rf.mu.Unlock()
	reply.Sussess = true
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


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.term
	IsLeader := (rf.job == Leader)

	// Your code here (2B).

	return index, (int)(term), IsLeader
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
	//* FIXME
	rand.Seed(time.Now().Unix())
	for !rf.killed() {
		time.Sleep(time.Duration(rand.Intn(130)) * time.Millisecond)
		//* Leader 不用判断超时，只有 Follower 需要判断当前心跳是否正常
		//* 不正常则发起选举
		if atomic.LoadInt64(&rf.job) == Follower {
			//* 随机超时时间，尽量避免同时发起选举
			if (time.Now().UnixMilli() - atomic.LoadInt64(&rf.lastreserve)) >= (int64)(rand.Intn(70) + 140) {
				//* 随机选举超时时间
				//* 我给我自己投一票
				var votes_tome int32 = 1
				//* 成为候选者
				//* 且自己的任期 + 1
				rf.mu.Lock()
				rf.term++
				atomic.SwapInt64(&rf.job, Candidate)
				rf.Vote_map[rf.term] = true
				args := RequestVoteArgs{}
				args.Term = rf.term
				args.CandidateID = (int64)(rf.me)
				log.Printf("id: %d term: %d start requestvote", rf.me, rf.term)
				rf.mu.Unlock()
				//* 随机选举超时时间，一旦超时，立即变为 Follower, 等待下一轮选举
				time.AfterFunc(time.Duration(300)*time.Millisecond, func() {
					rf.mu.Lock()
					if rf.job == Candidate {
						atomic.SwapInt64(&rf.job, Follower)
					}
					rf.mu.Unlock()
				})
				var i int32 = 0
				n := len(rf.peers)
				for ; i < (int32)(n) && atomic.LoadInt64(&rf.job) == Candidate; i++ {
					if rf.me == (int)(i) {
						continue
					}
					if atomic.LoadInt64(&rf.job) == Candidate {
						go func(ServerNumber int32) {
							reply := RequestVoteReply{}
							if atomic.LoadInt64(&rf.job) == Candidate {
								for atomic.LoadInt64(&rf.job) == Candidate {
									ok := rf.SendRequestVote(ServerNumber, &args, &reply)
									if ok {
										break
									}
								}
								rf.mu.Lock()
								//* 当有client 的任期大于当前服务器的任期时
								//* 自己变为 Follower
								//* 如果投票成功，则票数 + 1
								if reply.Success && rf.job == Candidate {
									votes_tome++
								}
								if reply.Term > rf.term && rf.job == Candidate {
									atomic.SwapInt64(&rf.job, Follower)
								}
								//* 如果当前获得集群的 1 / 2 投票
								//* 自己则成为当前集群的 Leader
								//* 并且给其他的 Follower 发送心跳
								if votes_tome >= ((int32)(len(rf.peers))/2 + 1) && rf.job == Candidate {
									log.Printf("id: %d term %d be Leader", rf.me, rf.term)
									atomic.SwapInt64(&rf.job, Leader)
								}
								rf.mu.Unlock()
							}
						}(int32(atomic.LoadInt32(&i)))
					}
					//* 如果选举成功，立即跳出并开始给 Follower 发起心跳
					rf.mu.Lock()
					if rf.job == Leader {
						rf.mu.Unlock()
						rf.job = Leader
						break
					}
					rf.mu.Unlock()
				}
			}
		}

		//* Leader 需要向其他 Follower 发送心跳
		if atomic.LoadInt64(&rf.job) == Leader {
			var i int32 = 0
			for ; i < (int32)(len(rf.peers)); i++ {
				if i == (int32)(rf.me) {
					continue
				}
				if atomic.LoadInt64(&rf.job) == Leader {
					go func(ServerNumber int32) {
						args := AppendEntriesArgs{}
						rf.mu.Lock()
						args.LeaderID = (int64)(rf.me)
						args.Term = rf.term
						rf.mu.Unlock()
						reply := AppendEntriesReply{}
						//* 不断重试 RPC, 发送心跳
						for atomic.LoadInt64(&rf.job) == Leader {
							args.Heartbeatime = time.Now().UnixMilli()
							ref := rf.SendAppendEntries(ServerNumber, &args, &reply)
							if ref {
								break
							}
						}
					}(int32(atomic.LoadInt32(&i)))
				}
			}
		}
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
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Ltime)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//* 集群开始的时候都是 follower, 任期为 0,
	rf.job = Follower
	rf.term = 0
	rf.lastreserve = 0
	rf.Vote_map = make(map[int64]bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.mu.Lock()
	go rf.ticker()
	rf.mu.Unlock()
	return rf
}
