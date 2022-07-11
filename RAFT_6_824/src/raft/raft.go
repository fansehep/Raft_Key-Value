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

	job         int64         //* 服务器当前角色，Leader or Follower
	term        int64         //* term 当前集群的任期
	lastreserve int64         //* 更新时间
	Vote_map    map[int64]int //* 每次只会给一个任期中给每个任期投票
	// Ktime_t     int64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Lognums []LogEntry

	CommitIndex int64 //* 已经提交的最后的一条日志的索引
	LastApplied int64 //* 已应用到服务器本地的最后一条日志条目的索引

	NextIndex  []int64
	MatchIndex []int64
}

type LogEntry struct {
	Command interface{}
	Term    int64
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
	Term         int64 //* Candidate 的任期
	CandidateID  int64 //* Candidate 的ID
	LastLogIndex int64 //* Candidate 最后的日志条目的索引值
	LastLogTerm  int64 //* Candidate 最后的日志条目的 term
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
	if args.Term > rf.term {
		//* 重复投票
		if rf.Vote_map[args.Term] == -1 || rf.Vote_map[args.Term] == (int)(args.CandidateID) {
			reply.Success = true
			rf.term = args.Term
			rf.lastreserve = time.Now().UnixMilli()
			log.Printf("id: %d term: %d vote %d %d", rf.me, rf.term, args.CandidateID, args.Term)
			rf.Vote_map[args.Term] = (int)(args.CandidateID)
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
	Term              int64      //* Leader 的任期
	LeaderID          int64      //* Leader ID
	Heartbeatime      int64      //* 心跳，用来更新时间
	PrevLogIndex      int64      //* 上一条日志记录的 index
	PrevLogTerm       int64      //* 上一条日志记录的 term
	ToSaveEntries     []LogEntry //* 需要被保存的日志条目
	LeaderCommitIndex int64      //* 领导人的已知的已提交的最高的日志的索引
}

type AppendEntriesReply struct {
	Term    int64 //* 回复者的任期
	Success bool  //* 是否成功
}

func (rf *Raft) SendAppendEntries(i int32, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	er := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.job != Leader {
		return er
	}

	return er
}

//* 心跳也需要判断任期，如果发起者的任期 < 当前的任期
//* 则返回error,
//* raft server中保存了 当前集群的 master id
//* 如果不同，心跳失败
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.term
	if args.Term < rf.term {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	//* prelogindex 和 prevlogterm 都需要匹配
	if args.PrevLogIndex != (int64)(len(rf.Lognums)-1) ||
		args.PrevLogTerm != (int64)(rf.Lognums[len(rf.Lognums)-1].Term) {
		reply.Success = false
		log.Printf("%d %d to %d %d append is not feet", args.LeaderID, args.Term, rf.me, rf.term)
		rf.mu.Unlock()
		return
	}
	//* 逐一对比要附加的日志 entries[] 是否和自己 prelogindex 之后的日志是否冲突
	/*
	 *  Lognums | 0 | 1 | 2 |
	 *  entries     | 0 | 1 | 2 |
	 *  i = 1 error =>
	 */
	j := 0
	for i := args.PrevLogIndex; i < (int64)(len(rf.Lognums)) && j < len(args.ToSaveEntries); i++ {
		if rf.Lognums[i].Term != args.ToSaveEntries[j].Term {
			//* 如果entries 日志中有位置发生冲突, 就将冲突之后的日志进行截断
			rf.Lognums = rf.Lognums[:(len(rf.Lognums) - (int)(i))]
			append_logentries := args.ToSaveEntries[:(len(args.ToSaveEntries) - (int)(j))]
			rf.Lognums = append(rf.Lognums)
			break
		}
		j++
	}
	//* 此时如果 j == len(args.ToSaveEntries) 表明所有的日志都已被匹配
	//*  1. 可能是重复的附加日志 RPC
	//*  2.
	rf.job = Follower
	rf.lastreserve = args.Heartbeatime
	rf.term = args.Term
	log.Printf("%d %d to %d %d append ok", args.LeaderID, args.Term, rf.me, rf.term)
	rf.mu.Unlock()
	reply.Success = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.term
	IsLeader := (rf.job == Leader)
	rf.mu.Unlock()
	//* 如果是 Follower
	if !IsLeader {
		return -1, (int)(term), false
	}
	index = len(rf.Lognums) - 1
	rf.Lognums = append(rf.Lognums, LogEntry{command, term})
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

func (rf *Raft) getPrevLogEntry(prevlogindex *int64, prevlogterm *int64) {
	*prevlogindex = (int64)(len(rf.Lognums) - 1)
	*prevlogterm = (int64)(rf.Lognums[*prevlogindex].Term)
}

func (rf *Raft) ticker() {
	n := len(rf.peers)
	RequestVoteTimer := time.NewTimer(100 * time.Millisecond)
	HeartBeatTimer := time.NewTimer(90 * time.Millisecond)

	for !rf.killed() {
		select {
		case <-HeartBeatTimer.C:
			go func() {
				//* 只有 leader 需要发送心跳
				rf.mu.Lock()
				if rf.job != Leader {
					rf.mu.Unlock()
					HeartBeatTimer.Reset(100 * time.Millisecond)
					return
				}
				rf.mu.Unlock()
				var i int32 = 0
				for ; i < (int32)(n); i++ {
					if i == (int32)(rf.me) {
						continue
					}
					go func(ServerNumber int32) {
						args := AppendEntriesArgs{}
						rf.mu.Lock()
						if rf.job != Leader {
							rf.mu.Unlock()
							return
						}
						args.LeaderID = (int64)(rf.me)
						rf.getPrevLogEntry(&args.PrevLogIndex, &args.PrevLogTerm)
						args.LeaderCommitIndex = rf.CommitIndex
						entries := rf.Lognums[rf.NextIndex[ServerNumber]:]
						//*  获取所需的复制的日志
						args.ToSaveEntries = make([]LogEntry, len(entries))
						copy(args.ToSaveEntries, entries)
						rf.mu.Unlock()

						reply := AppendEntriesReply{}
						args.Heartbeatime = time.Now().UnixMilli()
						ref := rf.SendAppendEntries(ServerNumber, &args, &reply)
						rf.mu.Lock()
						if rf.job != Leader {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						if ref {
							rf.mu.Lock()
							//* 如果发送心跳 接收者的任期 > 我自己的任期, 则变为 follower
							if reply.Term > rf.term {
								rf.job = Follower
								rf.term = args.Term
								rf.lastreserve = time.Now().UnixMilli()
							}
							rf.mu.Unlock()
							return
						}
					}(i)
				}
				HeartBeatTimer.Reset(100 * time.Millisecond)
			}()
		case <-RequestVoteTimer.C:
			go func() {
				rf.mu.Lock()
				//* 非 follower 不发起选举
				if rf.job != Follower {
					RequestVoteTimer.Reset((time.Duration((120 + rand.Intn(80)))) * time.Millisecond)
					rf.mu.Unlock()
					return
				}
				//* 未超时 不发起选举
				if time.Now().UnixMilli()-rf.lastreserve <= (int64)(350+rand.Intn(300)) {
					RequestVoteTimer.Reset((time.Duration((120 + rand.Intn(80)))) * time.Millisecond)
					rf.mu.Unlock()
					return
				}
				//* 投我的票数
				var vote_to_me int32 = 1
				//* 不投我的票数
				var no_vote_to_me int32 = 0
				//* 我投我自己一票
				rf.term++
				//* 变为 Candidate
				rf.job = Candidate
				rf.Vote_map[rf.term] = rf.me
				rf.lastreserve = time.Now().UnixMilli()
				args := RequestVoteArgs{}
				args.Term = rf.term
				args.CandidateID = (int64)(rf.me)
				log.Printf("id: %d term: %d start request vote", rf.me, rf.term)
				rf.mu.Unlock()
				var i int32 = 0
				for ; i < (int32)(n); i++ {
					rf.mu.Lock()
					if i == (int32)(rf.me) {
						rf.mu.Unlock()
						continue
					}
					if rf.job != Candidate {
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
					go func(ServerNumber int32) {
						reply := RequestVoteReply{}
						ok := rf.SendRequestVote(ServerNumber, &args, &reply)
						if !ok {
							reply.Success = false
						}
						if reply.Success {
							atomic.AddInt32(&vote_to_me, 1)
						} else {
							atomic.AddInt32(&no_vote_to_me, 1)
						}
						rf.mu.Lock()
						if rf.job != Candidate {
							rf.mu.Unlock()
							return
						}
						if reply.Term > rf.term {
							rf.job = Follower
							rf.lastreserve = time.Now().UnixMilli()
							rf.mu.Unlock()
							return
						}
						//* 当投我的票数 >= (n/2 + 1), 则变成 Leader
						if atomic.LoadInt32(&vote_to_me) >= ((int32)(n/2 + 1)) {
							log.Printf("id: %d term: %d be Leader ", rf.me, rf.term)
							rf.job = Leader
							rf.lastreserve = time.Now().UnixMilli()
							rf.mu.Unlock()
							return
						}
						//* 如果不投我的票数 > (n/2), 则立即变为 follower
						if atomic.LoadInt32(&no_vote_to_me) > (int32)(n/2) {
							rf.job = Follower
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					}(i)
				}
				RequestVoteTimer.Reset((time.Duration((rand.Intn(250) + 130))) * time.Millisecond)
			}()
		}
	}
	RequestVoteTimer.Stop()
	HeartBeatTimer.Stop()
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
	rf.Vote_map = make(map[int64]int)
	for i := 0; i < 2048; i++ {
		rf.Vote_map[(int64)(i)] = -1
	}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.Lognums = make([]LogEntry, 1)
	rf.Lognums[0].Term = 0
	rf.NextIndex = make([]int64, len(rf.peers))
	rf.MatchIndex = make([]int64, len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.mu.Lock()
	go rf.ticker()
	rf.mu.Unlock()
	return rf
}
