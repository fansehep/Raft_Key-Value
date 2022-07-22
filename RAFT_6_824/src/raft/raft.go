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
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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
	LogNums           []LogEntry    //* 每个服务器的日志记录
	MatchIndex        []int64       //* 已经同步的日志序号
	NextIndex         []int64       //* 对于远程节点来说，应该复制的下一个日志的下标
	LastApplied       int64         //* 最后被应用到状态机的日志条目索引
	LeaderCommitIndex int64         //* 已经应用到状态机的日志下标
	ApplyChan         chan ApplyMsg //* 返回给上层日志的通道
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
	buf := new(bytes.Buffer)
	data_t := labgob.NewEncoder(buf)
	data_t.Encode(rf.me)
	data_t.Encode(rf.job)
	data_t.Encode(rf.term)
	data_t.Encode(rf.Vote_map)
	data_t.Encode(rf.LogNums)
	data_t.Encode(rf.MatchIndex)
	data_t.Encode(rf.NextIndex)
	data_t.Encode(rf.LastApplied)
	data_t.Encode(rf.LeaderCommitIndex)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
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
	raft_t := Raft{}
	raft_t.Vote_map = make(map[int64]int)
	peers_n := len(rf.MatchIndex)
	raft_t.MatchIndex = make([]int64, peers_n)
	raft_t.NextIndex = make([]int64, peers_n)
	raft_t.LogNums = make([]LogEntry, len(rf.LogNums))
	buf := bytes.NewBuffer(data)
	d := labgob.NewDecoder(buf)
	if d.Decode(&(raft_t.me)) != nil ||
		d.Decode(&(raft_t.job)) != nil ||
		d.Decode(&(raft_t.term)) != nil ||
		d.Decode(&(raft_t.Vote_map)) != nil ||
		d.Decode(&(raft_t.LogNums)) != nil ||
		d.Decode(&(raft_t.MatchIndex)) != nil ||
		d.Decode(&(raft_t.NextIndex)) != nil ||
		d.Decode(&(raft_t.LastApplied)) != nil ||
		d.Decode(&(raft_t.LeaderCommitIndex)) != nil {
		log.Printf("error readPersist error!")
	} else {
		rf.me = raft_t.me
		rf.job = raft_t.job
		rf.term = raft_t.term
		rf.lastreserve = raft_t.lastreserve
		rf.Vote_map = raft_t.Vote_map
		rf.MatchIndex = raft_t.MatchIndex
		rf.NextIndex = raft_t.NextIndex
		rf.LastApplied = raft_t.LastApplied
		rf.LeaderCommitIndex = raft_t.LeaderCommitIndex
		rf.LogNums = raft_t.LogNums
	}
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
	CandidateID  int64 //* Candidate
	LastLogIndex int64 //* Candidate 的日志的最后下标
	LastLogTerm  int64 //* LastLogTerm LastLogIndex 所对应的日志的term
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
	reply.Success = false
	log.Printf("%d %d requestvote %d %d lastlogindex : %v lastlogterm %v", args.CandidateID, args.Term, rf.me, rf.term, args.LastLogIndex, args.LastLogTerm)
	if args.Term < rf.term {
		log.Printf("id: %d term: %d %v  no vote %d %d", rf.me, rf.term, rf.job, args.CandidateID, args.Term)
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.job = Follower
		rf.lastreserve = time.Now().UnixMilli()
		rf.persist()
	}
	if args.LastLogIndex == (int64)(len(rf.LogNums)-1) && args.LastLogTerm == (int64)(rf.LogNums[len(rf.LogNums)-1].Term) {
		rf.job = Follower
		rf.lastreserve = time.Now().UnixMilli()
		reply.Success = true
		rf.Vote_map[args.Term] = int(args.CandidateID)
		rf.persist()
		log.Printf("fun0 id: %d term: %d %v  vote %d %d curloglength %v curlogterm %v",
			rf.me, rf.term, rf.job, args.CandidateID, args.Term, len(rf.LogNums), rf.LogNums[len(rf.LogNums)-1].Term)
		rf.mu.Unlock()
		return
	} else if args.LastLogIndex < (int64)(len(rf.LogNums)-1) && args.LastLogTerm == (int64)(rf.LogNums[args.LastLogIndex].Term) {
		reply.Success = false
		log.Printf("fun1 id: %d term: %d no vote %d %d %v  curloglength %v curlogterm %v",
			rf.me, rf.term, rf.job, args.CandidateID, args.Term, len(rf.LogNums), rf.LogNums[len(rf.LogNums)-1].Term)
		rf.mu.Unlock()
		return
	}
	if (args.LastLogTerm) >= rf.LogNums[len(rf.LogNums)-1].Term {
		rf.term = args.Term
		rf.job = Follower
		rf.lastreserve = time.Now().UnixMilli()
		reply.Success = true
		rf.Vote_map[args.Term] = int(args.CandidateID)
		rf.persist()
		log.Printf("fun2 id: %d term: %d %v  vote %d %d curloglength %v curlogterm %v",
			rf.me, rf.term, rf.job, args.CandidateID, args.Term, len(rf.LogNums), rf.LogNums[len(rf.LogNums)-1].Term)
		rf.mu.Unlock()
		return
	}
	if (rf.Vote_map[args.Term] == -1 || rf.Vote_map[args.Term] == int(args.CandidateID)) && (((args.LastLogTerm) >= rf.LogNums[len(rf.LogNums)-1].Term) &&
		(args.LastLogIndex) >= (int64)(len(rf.LogNums)-1)) {
		rf.job = Follower
		rf.term = args.Term
		reply.Success = true
		rf.lastreserve = time.Now().UnixMilli()
		rf.Vote_map[rf.term] = int(args.CandidateID)
		rf.persist()
		log.Printf("fun3 id: %d term: %d %v  vote %d %d curloglength %v curlogterm %v",
			rf.me, rf.term, rf.job, args.CandidateID, args.Term, len(rf.LogNums), rf.LogNums[len(rf.LogNums)-1].Term)
		rf.mu.Unlock()
		return
	}
	if !reply.Success {
		log.Printf("fun4 id: %d term: %d no vote %d %d %v  curloglength %v curlogterm %v",
			rf.me, rf.term, rf.job, args.CandidateID, args.Term, len(rf.LogNums), rf.LogNums[len(rf.LogNums)-1].Term)
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term             int64      //* Leader 的任期
	LeaderID         int64      //* Leader ID
	Heartbeatime     int64      //* 心跳，用来更新时间
	ToSaveLogEntries []LogEntry //* 追加日志
	CommitIndex      int64      //* Leader 已经 commit 的 index
	PreLogIndex      int64      //* 给当前节点要附加日志的上一条的日志索引
	PreLogTerm       int64      //* 给当前节点要附加日志的上一条索引的 Term
}

type AppendEntriesReply struct {
	Term    int64 //* 回复者的任期
	Success bool  //* append 是否成功
}

//* 获取 Leader MatchIndex[] 中的中位数
//* 只有当一半的 Follower 都同步的日志
//* Leader才可以 Commit
func (rf *Raft) getMedianIndex() int64 {
	t_lognums := make([]int64, len(rf.peers))
	copy(t_lognums, rf.MatchIndex)
	sort.Slice(t_lognums, func(i, j int) bool {
		return t_lognums[i] < t_lognums[j]
	})
	for i := 0; i < len(t_lognums); i++ {
		log.Printf("MatchIndex[%d] = %d", i, t_lognums[i])
	}
	return t_lognums[len(t_lognums)/2]
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
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.Success = false
	if args.Term < rf.term {
		log.Printf("term %d %d to %d %d append fail", args.LeaderID, args.Term, rf.me, rf.term)
		return
	}
	rf.term = args.Term
	rf.job = Follower
	curloglength := (int64)(len(rf.LogNums))
	reply.Success = true
	rf.persist()
	/*
	 * 2. 0 >= 1 || 0 != 0
	 *
	 */

	if args.PreLogIndex >= curloglength {
		log.Printf("logindex %d %d to %d %d append fail\n args.PreLogIndex %v curloglength %v args.PreLogTerm %v",
			args.LeaderID, args.Term, rf.me, rf.term, args.PreLogIndex, curloglength,
			args.PreLogTerm)
		rf.lastreserve = time.Now().UnixMilli()
		reply.Success = false
		return
	}
	if args.PreLogTerm != (int64)(rf.LogNums[args.PreLogIndex].Term) {
		log.Printf("logunmatch %d %d to %d %d append fail\n args.PreLogIndex %v curloglength %v args.PreLogTerm %v curLogterm %v",
			args.LeaderID, args.Term, rf.me, rf.term, args.PreLogIndex, curloglength,
			args.PreLogTerm, rf.LogNums[args.PreLogIndex].Term)
		rf.lastreserve = time.Now().UnixMilli()
		reply.Success = false
		return
	}
	rf.lastreserve = time.Now().UnixMilli()
	var i int = 0
	var j int = (int)(args.PreLogIndex) + 1
	slogth := (int)(len(args.ToSaveLogEntries))
	/*
	 * 1 . 0 < 0 && 1 < 1
	 *		 nil lognums
	 * 2 . i = 0, j = 1, 0 < 1 && 1 < 1
	 * 3 . i = 0, j = 2, 0 < 0 && 2 < 2 slogth = 0
	 */
	for ; i < slogth && j < (int)(curloglength); i++ {
		if rf.LogNums[j].Term != args.ToSaveLogEntries[i].Term {
			rf.LogNums = rf.LogNums[:j]
			rf.LogNums = append(rf.LogNums, args.ToSaveLogEntries[i:]...)
			break
		}
		j++
	}
	//* 1. Leader 发来的日志全部匹配 但是我还是有多余的日志
	/*
	 * 1. i = 0, j = 1 0 == 0 && 1 < 1
	 * 2. i = 0, j = 1 0 == 1 && 1 < 1
	 * 3. i = 0, j = 2 0 == 0 && 2 < 2
	 */
	if i == slogth && j < (int)(curloglength) {
		rf.LogNums = rf.LogNums[:j]
	}
	//* 2. Leader 我发来的日志全部匹配, 即把剩下不匹配的日志加上即可
	/*
	 * 1. i = 0, j = 1, 1 == 1 && 0 < 0 false
	 * 2. i = 0, j = 1, 1 == 1 && 0 < 1 true
	 * 3. i = 0, j = 2, 2 == 2 && 0 < 0 false
	 */
	if j == int(curloglength) && i < slogth {
		rf.LogNums = append(rf.LogNums, args.ToSaveLogEntries[i:]...)
	}
	/*
	 * 1. -1 > -1
	 * 2. 1 > -1
	 *
	 */
	// if args.CommitIndex > rf.LeaderCommitIndex {
	// 	if args.PreLogIndex > args.CommitIndex {
	// 		rf.LeaderCommitIndex = args.CommitIndex
	// 	} else {
	// 		rf.LeaderCommitIndex = args.PreLogIndex
	// 	}
	// 	log.Printf("%v %v leadercommitindex %v",
	// 		rf.me, rf.term, rf.LeaderCommitIndex)
	// }
	// if len(rf.LogNums) > int(rf.LastApplied) {
	// 	//* 1 - 1 = 0 -> 1
	// 	//* 2 - 1 = 1
	// 	rf.LeaderCommitIndex = args.CommitIndex
	//*
	rf.persist()
	if args.CommitIndex > rf.LastApplied {
		if args.PreLogIndex > args.CommitIndex {
			rf.LeaderCommitIndex = args.CommitIndex
		} else {
			rf.LeaderCommitIndex = args.PreLogIndex
		}
	}
	k := rf.LastApplied
	for k < rf.LeaderCommitIndex {
		msg := ApplyMsg{}
		msg.Command = rf.LogNums[k+1].Command
		msg.CommandValid = true
		msg.CommandIndex = int(k + 1)
		rf.ApplyChan <- msg
		k++
		rf.LastApplied = k
	}
	rf.lastreserve = time.Now().UnixMilli()
	rf.persist()
	log.Printf("%d %d to %d %d append ok curlogsize %v curleadercommitindex %v",
		args.LeaderID, args.Term, rf.me, rf.term, len(rf.LogNums), rf.LeaderCommitIndex)
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
	term := rf.term
	IsLeader := (rf.job == Leader)
	//* 如果是 Follower
	if !IsLeader {
		return -1, (int)(term), false
	}
	rf.LogNums = append(rf.LogNums, LogEntry{command, rf.term})
	rf.persist()
	index := len(rf.LogNums) - 1
	return index, (int)(term), true
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
						rf.mu.Lock()
						//*1. 1 0
						//*2. 2 1
						rf.NextIndex[rf.me] = (int64)(len(rf.LogNums))
						rf.MatchIndex[rf.me] = (int64)(rf.NextIndex[rf.me]) - 1
						rf.mu.Unlock()
						continue
					}
					rf.mu.Lock()
					if rf.job != Leader {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					go func(ServerNumber int32) {
						args := AppendEntriesArgs{}
						rf.mu.Lock()
						if rf.job != Leader {
							rf.mu.Unlock()
							return
						}
						args.CommitIndex = rf.LeaderCommitIndex
						args.LeaderID = (int64)(rf.me)
						args.Term = rf.term
						/*
						 * 1. args.PreLogIndex = 0
						 *		args.PreLogTerm = ?
						 *    LogEntry [] size = 0
						 * 2. args.PreLogIndex = 0
						 *		args.PreLogTerm = ?
						 *    LogEntry [1] -> size = 1
						 * 3. args.PreLogIndex = 1
						 *		LogEntry [] nil
						 */
						//* args.PreLogIndex = 0
						args.PreLogIndex = rf.NextIndex[ServerNumber] - 1
						//* args.PreLogTerm = CurRaftTerm
						args.PreLogTerm = rf.LogNums[args.PreLogIndex].Term
						//* 1.  1  <  1
						//*
						var toSaveLogEntries []LogEntry
						if rf.NextIndex[ServerNumber] < (int64)(len(rf.LogNums)) {
							toSaveLogEntries = rf.LogNums[rf.NextIndex[ServerNumber]:]
						}
						args.ToSaveLogEntries = make([]LogEntry, len(toSaveLogEntries))
						copy(args.ToSaveLogEntries, toSaveLogEntries)
						log.Printf("leader %d to %d args.PreLogIndex: %d ars.PreLogterm: %d args.Commitindex %d nextindex %d matchindex %d curloglen %d tosavelogsize%d leadercommitidnex %v",
							rf.me, ServerNumber, args.PreLogIndex, args.PreLogTerm, args.CommitIndex, rf.NextIndex[ServerNumber], rf.MatchIndex[ServerNumber],
							len(rf.LogNums), len(args.ToSaveLogEntries), rf.LeaderCommitIndex)
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
							if reply.Term > rf.term {
								rf.job = Follower
								rf.term = args.Term
								rf.lastreserve = time.Now().UnixMilli()
								rf.persist()
								rf.mu.Unlock()
								return
							}
							if reply.Success {
								//* args.PreLogIndex + (int64)(len(args.LogNums))
								//* 1. 0 + 0 + 1 = 1
								//*    0 + 1 + 1 = 2
								//*    1 + 0 + 1 = 2
								rf.NextIndex[ServerNumber] =
									args.PreLogIndex + (int64)(len(args.ToSaveLogEntries)) + 1
								//* 1. 1 - 1 = 0
								//* 2. 2 - 1 = 1
								//*    2 - 1 = 1
								rf.MatchIndex[ServerNumber] =
									rf.NextIndex[ServerNumber] - 1
								rf.persist()
								//* 获取 MatchIndex 中的中位数
								//* 然后应用到状态机中
								newcommitindex := rf.getMedianIndex()
								if newcommitindex > rf.LeaderCommitIndex {
									rf.LeaderCommitIndex = newcommitindex
									rf.persist()
									k := rf.LastApplied
									for k < rf.LeaderCommitIndex {
										msg := ApplyMsg{}
										msg.Command = rf.LogNums[k+1].Command
										msg.CommandValid = true
										msg.CommandIndex = int(k + 1)
										rf.ApplyChan <- msg
										k++
										rf.LastApplied = k
									}
								}
								log.Printf("%d %d leader commitindex %d nextindex %v matchindex %v ",
									rf.me, rf.term, rf.LeaderCommitIndex, rf.NextIndex[ServerNumber],
									rf.MatchIndex[ServerNumber])
							} else {
								rf.NextIndex[ServerNumber] -= 2
								if rf.NextIndex[ServerNumber] <= 0 {
									rf.NextIndex[ServerNumber] = 1
								}
								rf.MatchIndex[ServerNumber] = 0
							}
							rf.mu.Unlock()
							return
						}
					}(i)
				}
				HeartBeatTimer.Reset(120 * time.Millisecond)
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
				if time.Now().UnixMilli()-rf.lastreserve <= (int64)(250+rand.Intn(250)) {
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
				rf.persist()
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
						rf.mu.Lock()
						args := RequestVoteArgs{}
						args.Term = rf.term
						args.CandidateID = (int64)(rf.me)
						/*
						 * 1. 0 0
						 */
						args.LastLogIndex = (int64)(len(rf.LogNums) - 1)
						args.LastLogTerm = (int64)(rf.LogNums[args.LastLogIndex].Term)
						rf.mu.Unlock()
						reply := RequestVoteReply{}
						rf.SendRequestVote(ServerNumber, &args, &reply)
						if reply.Success {
							atomic.AddInt32(&vote_to_me, 1)
						} else if !reply.Success {
							atomic.AddInt32(&no_vote_to_me, 1)
						}
						rf.mu.Lock()
						if reply.Term > rf.term {
							rf.job = Follower
							rf.term = reply.Term
							rf.lastreserve = time.Now().UnixMilli()
							rf.persist()
							rf.mu.Unlock()
							return
						}
						//* 当投我的票数 >= (n/2 + 1), 则变成 Leader
						if atomic.LoadInt32(&vote_to_me) >= ((int32)(n/2+1)) &&
							rf.job == Candidate {
							log.Printf("id: %d term: %d be Leader curloglength %v", rf.me, rf.term, len(rf.LogNums))
							rf.job = Leader
							rf.lastreserve = time.Now().UnixMilli()
							//* 成为 Leader 之后
							//* 所有 NextIndex[i] 都变为自己的 len(rf.LogNums)
							CurLogNumsLength := len(rf.LogNums)
							log.Printf("rf.LogNumslength = %d", CurLogNumsLength)
							for k := 0; k < n; k++ {
								rf.NextIndex[k] = (int64)(CurLogNumsLength)
							}
							//* 有变化
							rf.persist()
							rf.mu.Unlock()
							return
						}
						//* 如果不投我的票数 > (n/2), 则立即变为 follower
						if atomic.LoadInt32(&no_vote_to_me) > (int32)(n/2) {
							rf.job = Follower
							//* 有变化
							log.Printf("id: %d term: %d be follower novote %v", rf.me, rf.term, atomic.LoadInt32(&no_vote_to_me))
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					}(i)
				}
				RequestVoteTimer.Reset((time.Duration((rand.Intn(200) + 100))) * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//* 集群开始的时候都是 follower, 任期为 0,
	rf.job = Follower
	rf.term = 0
	rf.lastreserve = 0
	rf.Vote_map = make(map[int64]int)
	rf.MatchIndex = make([]int64, len(peers))
	rf.NextIndex = make([]int64, len(peers))
	rf.LogNums = make([]LogEntry, 1)
	rf.LogNums[0].Term = 0
	rf.LogNums[0].Command = nil
	rf.LastApplied = 0
	rf.LeaderCommitIndex = -1
	rf.ApplyChan = applyCh
	for i := 0; i < 1024; i++ {
		rf.Vote_map[(int64)(i)] = -1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.job = Follower
	log.Printf("hi new raft! id : %v term : %v loglen : %d", rf.me, rf.term, len(rf.LogNums))
	// start ticker goroutine to start elections
	rf.mu.Lock()
	go rf.ticker()
	rf.mu.Unlock()
	return rf
}
