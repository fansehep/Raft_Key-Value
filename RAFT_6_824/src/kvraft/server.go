package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/sasha-s/go-deadlock"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT    = "Put"
	GET    = "Get"
	APPEND = "Append"
)

type Op struct {
	Opertype string
	Key      string
	Value    string
	Index    int64
	ClientID int64
}

type KVServer struct {
	mu           deadlock.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	ApplyIndex   int   // Leader 应用的日志下标
	maxraftstate int   // snapshot if log grows this big
	KvStore      map[string]string
	//* 保存每个 client 的最后一次执行的 index, index++
	OperMap map[int64]int64
	//* key: index(单个command的id) + term(单个的term) + clientid(客户端的id)
	//* 通过该channel拿到真实的数据
	ResultChan map[string]chan Result
	//* 上一次获取快照的下标
	LastSnapShotIndex int
	// Your definitions here.
}

type Result struct {
	Key   string
	Value string
	ERROR string
}

func ToStringChankey(index int, term int, clientid int64) string {
	return fmt.Sprintf("%v_%v_%v", index, term, clientid)
}

/**
 * TODO: 可以实现只读操作, 只读操作可以快速返回, 但是如果没有交给raft
 * , 则可能返回脏数据
 * 原因是因为: 当一个老的Leader 由于发生网络分区之后, 被集群所隔离,
 * 按照论文中的实现, 他可能与client的连接是好的, 但他仍然还是Leader,
 * 那么就会直接返回旧数据, 我们如果要实现只读语义, 必须要确保当前节点是 1/2
 * 中的其中之一. 一个方法是, 等待 raft的一轮心跳, 且受到了 1 /2 以上的节点
 * 的正常回复, 则可以认为当前节点就是集群的 Leader. 正常返回即可,
 * 这里我认为我们其实也可以从 Follower上读取数据, 比如说, 我们可以让当前Follower
 * 等待一轮心跳的确认, 那么也认为该Follower是健康的.
 */

func (kv *KVServer) ExecuteCommand(msg raft.ApplyMsg) {
	var res Result
	op := msg.Command.(Op)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//* 如果是过时的请求, 标记为 OK
	if kv.ApplyIndex >= msg.CommandIndex {
		res.ERROR = OK
		return
	}

	//* 获得该client最近一次拿到的 ClientID
	csquencen, ok := kv.OperMap[op.ClientID]
	//* 如果是已经过去的请求, 并且请求是 APPEND or PUT
	//* Server 应该不处理该请求
	if ok && csquencen >= op.Index && op.Opertype != GET {
		res.ERROR = OK
	} else if op.Opertype == PUT {
		kv.KvStore[op.Key] = op.Value

		res.ERROR = OK
		kv.OperMap[op.ClientID] = op.Index
	} else if op.Opertype == GET {

		_, isok := kv.KvStore[op.Key]
		if isok {
			res.Value = kv.KvStore[op.Key]
			res.ERROR = OK
		} else {
			res.ERROR = ErrNoKey
			res.Value = ""
		}
		kv.OperMap[op.ClientID] = op.Index
	} else if op.Opertype == APPEND {
		kv.KvStore[op.Key] += op.Value
		res.ERROR = OK
		kv.OperMap[op.ClientID] = op.Index
	}

	kv.ApplyIndex = msg.CommandIndex
	resultchan, ok := kv.ResultChan[ToStringChankey(msg.CommandIndex, msg.CommandTerm, op.ClientID)]

	if ok {
		select {
		case resultchan <- res:
		case <-time.After(10 * time.Millisecond):
		}
	}

	DEBUG(dInfo, "S%v Server id: %v apply op: %v key: %v value: %v index: %v ",
		kv.me, kv.me, op.Opertype, op.Key, res.Value, op.Index)
}

func (kv *KVServer) CommandBg() {
	for !kv.killed() {
		for msg := range kv.applyCh {

			if msg.SnapshotValid {
				kv.ApplySnapShotMsg(msg)
				continue
			}
			kv.ExecuteCommand(msg)
			
			kv.ExecuteSnapshot()
		}
	}
}

func (kv *KVServer) ApplySnapShotMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.ReadStateFromSnapShot(msg.Snapshot)
		kv.ApplyIndex = msg.SnapshotIndex
		kv.LastSnapShotIndex = kv.ApplyIndex
	}
}

func (kv *KVServer) ExecuteSnapshot() {
	if kv.maxraftstate == -1 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DEBUG(dInfo, "S%v Server id: %v execute snapshot kv.maxraftstate: %v currentsize %v ApplyIndex: %v",
		kv.me, kv.me, kv.maxraftstate, kv.rf.GetCurrentStateSize(), kv.ApplyIndex)
	if int(float64(kv.maxraftstate)*0.8) <= kv.rf.GetCurrentStateSize() &&
		kv.ApplyIndex-kv.LastSnapShotIndex >= 20 {
		snapshot := kv.GeneredSnapshot()
		kv.rf.Snapshot(kv.ApplyIndex, snapshot)
		kv.LastSnapShotIndex = kv.ApplyIndex
	}
}

//* 生成快照, 需要保存当前的map, 当前的每个client的最后一个命令的index

func (kv *KVServer) GeneredSnapshot() []byte {
	buf := new(bytes.Buffer)

	encode := labgob.NewEncoder(buf)

	encode.Encode(kv.KvStore)
	encode.Encode(kv.OperMap)
	encode.Encode(kv.ApplyIndex)

	return buf.Bytes()
}

//* 恢复状态
func (kv *KVServer) ReadStateFromSnapShot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(buf)

	new_kvstroe := make(map[string]string)
	new_ClientOperMap := make(map[int64]int64)
	new_appliedIndex := 0
	d.Decode(&new_kvstroe)
	d.Decode(&new_ClientOperMap)
	d.Decode(&new_appliedIndex)

	kv.KvStore = new_kvstroe
	kv.OperMap = new_ClientOperMap
	kv.ApplyIndex = new_appliedIndex
}

func (kv *KVServer) StartCommand(opt Op) Result {
	var result Result

	// 只读操作直接返回即可

	CommandIndex, Term, isleader := kv.rf.Start(opt)
	if !isleader {
		result.ERROR = ErrWrongLeader
		return result
	}

	kv.mu.Lock()
	resultChan := make(chan Result)
	str := ToStringChankey(CommandIndex, Term, opt.ClientID)
	kv.ResultChan[str] = resultChan
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.ResultChan, str)
		close(resultChan)
		kv.mu.Unlock()
	}()

	select {
	case result = <-resultChan:
		{
			return result
		}
	case <-time.After(time.Second * 1):
		{
			result.ERROR = ExpiredReq
			return result
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	oper := Op{
		Opertype: GET,
		Key:      args.Key,
		Index:    args.Index,
		ClientID: args.ClientId,
	}

	//* 每次 Get 请求, 超时就让客户端重试
	res := kv.StartCommand(oper)
	reply.Err = Err(res.ERROR)
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	lastindex, ok := kv.OperMap[args.ClientId]
	if ok && lastindex >= args.Index {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	//* 存在, 该命令重复
	oper := Op{
		Opertype: args.Op,
		Index:    args.Index,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientId,
	}
	res := kv.StartCommand(oper)
	reply.Err = Err(res.ERROR)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	// 最大的raft日志大小
	//
	kv.maxraftstate = maxraftstate
	kv.KvStore = make(map[string]string)
	// id -> command index
	kv.OperMap = make(map[int64]int64)
	kv.ResultChan = make(map[string]chan Result)
	kv.ApplyIndex = 0
	// You may need initialization code here.

	if persister.SnapshotSize() > 0 {
		kv.ReadStateFromSnapShot(persister.ReadSnapshot())
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.CommandBg()
	return kv
}
