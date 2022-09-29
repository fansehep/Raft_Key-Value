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
	"bytes"
	"math"
	"math/rand"
	"sort"

	"6.824/labgob"

	//	"bytes"

	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Role string

const (
	RoleFollower  = "Follower"
	RoleCandidate = "Candidate"
	RoleLeader    = "Leader"

	minElectionIntervalTime = 700
	maxElectionIntervalTime = 1000
	broadcastIntervalTime   = 100
)

func getRandomElectionIntervalTime() int64 {
	return minElectionIntervalTime + rand.Int63n(maxElectionIntervalTime-minElectionIntervalTime)
}

type Log struct {
	Entries           []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// LogEntry 日志项
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (lo *Log) get(i int) LogEntry {
	if i == lo.LastIncludedIndex {
		return LogEntry{Term: lo.LastIncludedTerm, Index: lo.LastIncludedIndex}
	}
	if i > lo.LastIncludedIndex+lo.size() {
		panic("ERROR: index greater than log length!\n")
	}
	if i < lo.LastIncludedIndex {
		panic("ERROR: index smaller than log snapshot!\n")
	}
	return lo.Entries[i-lo.LastIncludedIndex-1]
}

func (lo *Log) size() int {
	return len(lo.Entries)
}

func (lo *Log) getLastLogIndex() int {
	return lo.LastIncludedIndex + lo.size()
}

func (lo *Log) getLastLogTerm() int {
	return lo.get(lo.getLastLogIndex()).Term
}

func (lo *Log) findFirstLogIndex(term int) int {
	l, r := 0, lo.size()
	for l < r {
		mid := l + (r-l)/2
		if lo.Entries[mid].Term >= term {
			r = mid
		} else if lo.Entries[mid].Term < term {
			l = mid + 1
		}
	}
	return l + 1 + lo.LastIncludedIndex
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 选举相关
	role     Role //当前节点角色
	leaderID int  //leaderID

	// 所有节点持久化数据
	currentTerm int    // 当前节点term
	voteFor     int    // 当前term投票给谁
	log         Log    // 日志
	snapshot    []byte // 快照

	// 所有节点易失性数据
	commitIndex int // 已知最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// Leader节点易失性数据
	nextIndex  []int // 每个Follower的log同步起点索引(初始为leader log的最后一项)
	matchIndex []int // 每个Follower的log同步进度(初始为0)，和nextIndex强关联

	lastActiveTime int64 //最后活跃时间(刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票)
	applyCh        chan ApplyMsg

	// 用来保证快照和log之间有序
	readySnapshotLastIndex int
	snapshotCh             chan ApplyMsg
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = RoleFollower
	rf.currentTerm = term
	rf.voteFor = -1
	rf.leaderID = -1
	DPrintf("become follower, me: %d, term: %d, lastTerm: %d, lastLog: %d, commitIndex: %d\n", rf.me, rf.currentTerm, rf.log.getLastLogTerm(), rf.log.getLastLogIndex(), rf.commitIndex)
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.role = RoleCandidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.leaderID = -1
	DPrintf("become candidate, me: %d, term: %d, lastTerm: %d, lastLog: %d, commitIndex: %d", rf.me, rf.currentTerm, rf.log.getLastLogTerm(), rf.log.getLastLogIndex(), rf.commitIndex)
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	nextIndex := make([]int, 0, len(rf.peers))
	matchIndex := make([]int, 0, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		nextIndex = append(nextIndex, rf.log.getLastLogIndex()+1)
		matchIndex = append(matchIndex, 0)
	}

	rf.role = RoleLeader
	rf.leaderID = rf.me
	rf.nextIndex = nextIndex
	rf.matchIndex = matchIndex

	DPrintf("become leader, me: %d, term: %d, lastTerm: %d, lastLog: %d, commitIndex: %d", rf.me, rf.currentTerm, rf.log.getLastLogTerm(), rf.log.getLastLogIndex(), rf.commitIndex)
	go rf.broadcastLoop(rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
// * 线程安全
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == RoleLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.log.LastIncludedIndex = 0
		rf.log.LastIncludedTerm = -1
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)

	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

func (rf *Raft) persistStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)

	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		rf.readySnapshotLastIndex = rf.log.LastIncludedIndex
		return false
	}

	if lastIncludedIndex <= rf.log.getLastLogIndex() {
		rf.log.Entries = append([]LogEntry{}, rf.log.Entries[lastIncludedIndex-rf.log.LastIncludedIndex:]...)
	} else {
		rf.log.Entries = nil
	}

	rf.log.LastIncludedIndex = lastIncludedIndex
	rf.log.LastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.persistStateAndSnapshot()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("me %d: Snapshot index: %d", rf.me, index)

	if index <= rf.log.LastIncludedIndex {
		return
	}

	indexEntry := rf.log.get(index)
	rf.log.Entries = append([]LogEntry{}, rf.log.Entries[index-rf.log.LastIncludedIndex:]...)
	rf.log.LastIncludedIndex = index
	rf.log.LastIncludedTerm = indexEntry.Term

	rf.snapshot = snapshot
	rf.persistStateAndSnapshot()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 快速恢复
	XTerm  int
	XIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("voteReq me: %d, term: %d, voteFor: %d, lastLogIndex: %d, lastLogTerm: %d, req: %+v", rf.me, rf.currentTerm, rf.voteFor, rf.log.getLastLogIndex(), rf.log.getLastLogTerm(), args)
	defer func() {
		DPrintf("voteRsp me: %d, term: %d, voteFor: %d, lastLogIndex: %d, lastLogTerm: %d, req: %+v, rsp: %+v", rf.me, rf.currentTerm, rf.voteFor, rf.log.getLastLogIndex(), rf.log.getLastLogTerm(), args, reply)
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		lastLogIndex := rf.log.getLastLogIndex()
		lastLogTerm := rf.log.getLastLogTerm()

		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.voteFor = args.CandidateId
			rf.lastActiveTime = time.Now().UnixNano() / 1e6
			rf.persist()

			reply.Term = args.Term
			reply.VoteGranted = true
			return
		}

		reply.Term = args.Term
		reply.VoteGranted = false
		return

	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm ||
		args.Term == rf.currentTerm && rf.role != RoleFollower {
		rf.becomeFollower(args.Term)
	}

	// 网络不稳定 滞后请求
	if args.PrevLogIndex < rf.log.LastIncludedIndex ||
		(args.PrevLogIndex == rf.log.LastIncludedIndex && args.PrevLogTerm != rf.log.LastIncludedTerm) {
		return
	}

	// 刷新活跃时间
	rf.leaderID = args.LeaderId
	rf.lastActiveTime = time.Now().UnixNano() / 1e6

	// follower数据不够
	if args.PrevLogIndex > rf.log.getLastLogIndex() {
		reply.XTerm = -1
		reply.XIndex = rf.log.getLastLogIndex()
		return
	}

	// 和follower数据对不齐
	if args.PrevLogIndex > 0 && rf.log.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.XTerm = rf.log.get(args.PrevLogIndex).Term
		reply.XIndex = rf.log.findFirstLogIndex(reply.XTerm)
		return
	}

	// 可能出现乱序或者重入问题
	reply.Success = true

	misMatchIndex := -1
	for i, entry := range args.Entries {
		if args.PrevLogIndex+i >= rf.log.getLastLogIndex() || rf.log.get(args.PrevLogIndex+i+1).Term != entry.Term {
			misMatchIndex = i
			break
		}
	}

	if misMatchIndex != -1 {
		rf.log.Entries = append(rf.log.Entries[:args.PrevLogIndex+misMatchIndex-rf.log.LastIncludedIndex], args.Entries[misMatchIndex:]...)
		DPrintf("follower log me: %d, leader: %d, log: %+v, args: %+v", rf.me, args.LeaderId, rf.log, args)
		rf.persist()
	}

	rf.commitIndex = int(math.Min(float64(rf.log.getLastLogIndex()), float64(args.LeaderCommit)))
	DPrintf("commit log me: %d, leader: %d, term: %d, leaderCommit: %d, commit: %d", rf.me, args.LeaderId, args.Term, args.LeaderCommit, rf.commitIndex)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("server %d receive InstallSnapshot from %d\n", rf.me, args.LeaderID)
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm ||
		args.Term == rf.currentTerm && rf.role != RoleFollower {
		rf.becomeFollower(args.Term)
	}

	if args.LastIncludedIndex <= rf.log.LastIncludedIndex {
		return
	}
	// 刷新活跃时间
	rf.leaderID = args.LeaderID
	rf.lastActiveTime = time.Now().UnixNano() / 1e6

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// 用来保证快照和log之间有序
	rf.readySnapshotLastIndex = args.LastIncludedIndex
	go func() { rf.snapshotCh <- msg }()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RoleLeader {
		return -1, -1, false
	}

	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.log.getLastLogIndex() + 1,
	}
	rf.log.Entries = append(rf.log.Entries, entry)
	rf.persist()
	DPrintf("leader log me: %d, term: %d, log: %+v", rf.me, rf.currentTerm, rf.log)

	term = entry.Term
	index = entry.Index

	rf.broadcast()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) election() {
	type replyChanObj struct {
		peer  int
		ok    bool
		reply *RequestVoteReply
	}

	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.getLastLogIndex(),
		LastLogTerm:  rf.log.getLastLogTerm(),
	}

	voteReplyChan := make(chan *replyChanObj, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(peer, req, reply)
			voteReplyChan <- &replyChanObj{
				peer:  peer,
				ok:    ok,
				reply: reply,
			}
		}(i)
	}

	go func(term int) {
		voteCount := 1
		becomeLeader := false
		for i := 0; i < len(rf.peers)-1; i++ {
			reply := <-voteReplyChan
			if reply.ok && reply.reply.Term > term {
				rf.mu.Lock()
				if reply.reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.reply.Term)
				}
				rf.mu.Unlock()
			}

			if reply.ok && reply.reply.VoteGranted {
				voteCount++
			}

			if voteCount > len(rf.peers)/2 && !becomeLeader {
				rf.mu.Lock()
				if rf.currentTerm != term || rf.role != RoleCandidate {
					rf.mu.Unlock()
					return
				}
				rf.becomeLeader()
				becomeLeader = true
				rf.mu.Unlock()
			}
		}
	}(rf.currentTerm)
}

func (rf *Raft) broadcast() {
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		if rf.nextIndex[peer] <= rf.log.LastIncludedIndex {
			rf.doInstallSnapshot(peer)
			continue
		}

		rf.doAppendEntriesReq(peer)
	}
}

func (rf *Raft) doInstallSnapshot(peer int) {
	req := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.log.LastIncludedIndex,
		LastIncludedTerm:  rf.log.LastIncludedTerm,
		Data:              rf.snapshot,
	}
	reply := &InstallSnapshotReply{}

	go func() {
		ok := rf.sendInstallSnapshot(peer, req, reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		if rf.currentTerm != req.Term || rf.currentTerm != reply.Term {
			return
		}

		rf.nextIndex[peer] = req.LastIncludedIndex + 1
		rf.matchIndex[peer] = req.LastIncludedIndex

	}()
}

func (rf *Raft) doAppendEntriesReq(peer int) {
	req := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}

	req.PrevLogIndex = rf.nextIndex[peer] - 1
	req.PrevLogTerm = rf.log.get(req.PrevLogIndex).Term

	req.Entries = append(req.Entries, rf.log.Entries[req.PrevLogIndex-rf.log.LastIncludedIndex:]...)

	go func() {
		ok := rf.sendAppendEntries(peer, req, reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		if rf.currentTerm != req.Term || rf.currentTerm != reply.Term {
			return
		}

		if !reply.Success {
			// 网络不稳定滞后请求
			if reply.XIndex == -1 {
				return
			}

			// 更新nextIndex， 同上，需要防重入(重入无非就是再重新发一次已经发过了的)
			if reply.XTerm == -1 {
				rf.nextIndex[peer] = reply.XIndex + 1
			} else if reply.XIndex < rf.log.LastIncludedIndex {
				rf.nextIndex[peer] = reply.XIndex
			} else if rf.log.get(reply.XIndex).Term == reply.XTerm {
				rf.nextIndex[peer] = reply.XIndex + 1
			} else if rf.log.get(reply.XIndex).Term != reply.XTerm {
				rf.nextIndex[peer] = reply.XIndex
			}
			return
		}

		// 在start的时候调用broadcast可能导致重入逻辑（一条log发送两次，都会被follower应用），所以这里算nextIndex的时候不能用nextIndex算
		rf.nextIndex[peer] = req.PrevLogIndex + len(req.Entries) + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1

		// 查找超过一半Follower拥有的index
		sortMatchIndex := make([]int, 0, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				sortMatchIndex = append(sortMatchIndex, rf.log.getLastLogIndex())
				continue
			}
			sortMatchIndex = append(sortMatchIndex, rf.matchIndex[i])
		}
		sort.Ints(sortMatchIndex)
		newCommitIndex := sortMatchIndex[len(rf.peers)/2]
		// leader只能提交在自己term里的日志
		if newCommitIndex > rf.commitIndex && rf.log.get(newCommitIndex).Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
		}
		return
	}()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastActiveTime = time.Now().UnixNano() / 1e6
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.lastApplied = rf.log.LastIncludedIndex
	rf.snapshotCh = make(chan ApplyMsg)

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.electionLoop()
	go rf.applyLoop()

	return rf
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		now := time.Now().UnixNano() / 1e6
		needIntervalTime := getRandomElectionIntervalTime()
		intervalTime := now - rf.lastActiveTime
		if intervalTime < needIntervalTime {
			rf.mu.Unlock()

			time.Sleep(time.Duration(needIntervalTime-intervalTime) * time.Millisecond)
			continue
		}

		if rf.role == RoleLeader {
			rf.mu.Unlock()

			time.Sleep(5 * time.Millisecond)
			continue
		}

		rf.becomeCandidate()

		rf.election()

		rf.lastActiveTime = time.Now().UnixNano() / 1e6

		rf.mu.Unlock()
	}
}

func (rf *Raft) broadcastLoop(term int) {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		rf.broadcast()

		rf.mu.Unlock()
		time.Sleep(broadcastIntervalTime * time.Millisecond)
	}
}

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		var logs []ApplyMsg
		rf.mu.Lock()

		for rf.commitIndex > rf.lastApplied && rf.lastApplied >= rf.readySnapshotLastIndex {
			rf.lastApplied += 1
			logs = append(logs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log.get(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.currentTerm,
			})
		}

		rf.mu.Unlock()

		for _, msg := range logs {
			rf.applyCh <- msg
		}

		// 用来保证快照和log之间有序
		select {
		case snapshotMsg := <-rf.snapshotCh:
			rf.applyCh <- snapshotMsg
		default:
		}

		time.Sleep(1 * time.Millisecond)
	}

}

func (rf *Raft) GetCurrentStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
