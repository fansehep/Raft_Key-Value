package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId int // 当前LeaderID
	ServerN  int // 远程服务器的数量
	Me       int // 个人ID
}

var i int

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.LeaderId = 0
	ck.ServerN = len(ck.servers)
	ck.Me = i
	i++
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	DEBUG(dInfo, "S%v client get key: %v",
		1, args.Key)
	for {
		reply := GetReply{}
		ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply)
		if reply.Err == ErrWrongLeader {
			ck.LeaderId = (ck.LeaderId + 1) % ck.ServerN
			DEBUG(dInfo, "S%v client get key: %v error: %v \n", ck.Me, args.Key, reply.Err)
		} else {
			DEBUG(dInfo, "S%v client get key: %v value: %v ok: %v CurrentLeader: %v\n",
				ck.Me, args.Key, reply.Value, reply.Err, ck.LeaderId)
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Index: nrand(),
	}
	DEBUG(dInfo, "S%v client put append key: %v value: %v op: %v\n",
		1, args.Key, args.Value, args.Op)
	for {
		reply := PutAppendReply{}
		ck.servers[ck.LeaderId].Call("KVServer.PutAppend", &args, &reply)
		//* 错误的Leader请求
		if reply.Err == ErrWrongLeader {
			DEBUG(dInfo, "S%v client put append key: %v value: %v Err: %v currleaderId: %v \n",
				ck.Me, args.Key, args.Value, reply.Err, ck.LeaderId)
			ck.LeaderId = (ck.LeaderId + 1) % ck.ServerN
			//* 重复的请求, 阻止客户端
		} else if reply.Err == RepeatedReq {
			DEBUG(dInfo, "S%v client put append key: %v value: %v Err: %v currleaderId: %v \n",
				ck.Me, args.Key, args.Value, reply.Err, ck.LeaderId)
			break
			//* 执行成功, 返回即可
		} else if reply.Err == OK {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
