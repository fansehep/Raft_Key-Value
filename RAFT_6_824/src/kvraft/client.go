package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId int   // 当前LeaderID
	ServerN  int   // 远程服务器的数量
	Me       int64 // 当前 client ID
	ReqIndex int64 // 请求ID
}

var i int64 = 0

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
	ck.Me = nrand()
	i++
	ck.ReqIndex = 0
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
	ck.ReqIndex++
	args := GetArgs{
		Key:      key,
		Index:    ck.ReqIndex,
		ClientId: ck.Me,
	}
	DEBUG(dInfo, "S%v client get key: %v",
		1, args.Key)
	for {
		reply := GetReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply)
		//* 错误的 Leader 需要重发
		DEBUG(dInfo, "S%v client get key: %v error: %v \n", ck.Me, args.Key, reply.Err)
		//* 过时的请求 请重复
		if ok && reply.Err == ErrWrongLeader {
			ck.LeaderId = (ck.LeaderId + 1) % ck.ServerN
		}
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
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
	ck.ReqIndex++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Index:    ck.ReqIndex,
		ClientId: ck.Me,
	}
	DEBUG(dInfo, "S%v client put append key: %v value: %v op: %v\n",
		1, args.Key, args.Value, args.Op)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.PutAppend", &args, &reply)
		//* 错误的Leader请求
		if ok && reply.Err == OK {
			return
		}

		ck.LeaderId = (ck.LeaderId + 1) % ck.ServerN
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
