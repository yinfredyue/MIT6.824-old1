package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	currLeader int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.currLeader = 0 // Initialized to the 1st server

	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	server := ck.currLeader
	getArgs := GetArgs{Key: key, OpID: nrand()}
	for {
		getReply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &getArgs, &getReply)

		// handle reply
		if ok {
			switch getReply.Err {
			case OK:
				ck.currLeader = server
				return getReply.Value
			case ErrNoKey:
				ck.currLeader = server
				return ""
			case ErrWrongLeader:
				// retry
			}
		}
		server = (server + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	server := ck.currLeader
	putAppendArgs := PutAppendArgs{Key: key, Value: value, OpID: nrand(), Op: op}
	for {
		putAppendReply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)

		// handle reply
		if ok {
			switch putAppendReply.Err {
			case OK:
				ck.currLeader = server
				return
			case ErrNoKey:
				DPrintf("This should not happen")
			case ErrWrongLeader:
				// retry
			}
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
