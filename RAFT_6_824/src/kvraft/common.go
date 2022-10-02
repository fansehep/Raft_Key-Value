package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	RepeatedReq    = "RepeatedReq"
	ExpiredReq     = "ExpiredReq" //* 过时的请求
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//单个操作的 uuid, 标记, 防止 put 请求重复
	Index    int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key   string
	Index int64
	// 客户端 id
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
