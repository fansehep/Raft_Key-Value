package kvraft

import (
	"log"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/sasha-s/go-deadlock"
)

const Debug = false

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
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	KvStore      map[string](string)
	OperMap      map[int64]bool
	// Your definitions here.
}

func (kv *KVServer) FollowerBg() {
	for kv.killed() {
		_, isleader := kv.rf.GetState()
		var comm raft.ApplyMsg
		if !isleader {
			comm = <-kv.applyCh
		} else {
			continue
		}
		opcomm := ((comm.Command).(Op))
		// 防止多次操作
		_, ok := kv.OperMap[opcomm.Index]
		if ok {
			continue
		}
		DEBUG(dInfo, "%S id: %v op: %v key: %v value: %v",
			kv.me, opcomm.Opertype, opcomm.Key, opcomm.Value)
		if opcomm.Opertype == PUT {
			kv.KvStore[opcomm.Key] = opcomm.Value
			kv.OperMap[opcomm.Index] = true
		} else if opcomm.Opertype == APPEND {
			str := kv.KvStore[opcomm.Key]
			str = str + opcomm.Value
			kv.KvStore[opcomm.Key] = str
			kv.OperMap[opcomm.Index] = true
		}
	}
}

func (kv *KVServer) Excute() {
	comm := <-kv.applyCh
	// PUT 请求
	op := ((comm.Command).(Op))
	_, ok := kv.OperMap[op.Index]
	if ok {
		return
	}
	DEBUG(dInfo, "S%v Server id: %v op: %v, key: %v value: %v Index: %v \n",
		kv.me, kv.me, op.Opertype, op.Key, op.Value, op.Index)
	if op.Opertype == GET {
		// PUT 请求
	} else if op.Opertype == PUT {
		kv.KvStore[op.Key] = op.Value
		// GET 请求
	} else if op.Opertype == APPEND {
		_, ok := kv.KvStore[op.Key]
		// key 存在
		if ok {
			str := kv.KvStore[op.Key]
			str = str + op.Value
			kv.KvStore[op.Key] = str
			// key 不存在
		} else {
			kv.KvStore[op.Key] = op.Value
		}
	}
	if op.Opertype == PUT || op.Opertype == APPEND {
		kv.OperMap[op.Index] = true
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	oper := Op{
		Opertype: GET,
		Key:      args.Key,
	}
	kv.rf.Start(oper)
	kv.Excute()
	reply.Value = kv.KvStore[oper.Key]
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	_, ok := kv.OperMap[args.Index]
	//* 存在, 该命令重复
	if ok {
		reply.Err = RepeatedReq
		return
	}
	oper := Op{
		Opertype: args.Op,
		Index:    args.Index,
		Key:      args.Key,
		Value:    args.Value,
	}
	kv.rf.Start(oper)
	kv.Excute()
	reply.Err = OK
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
	kv.maxraftstate = maxraftstate
	kv.KvStore = make(map[string]string)
	kv.OperMap = make(map[int64]bool)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// 等待 raft 选举出Leader
	// You may need initialization code here.
	go kv.FollowerBg()
	return kv
}
