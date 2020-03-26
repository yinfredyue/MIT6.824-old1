package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	// opType used in Op
	getOp    int = 0
	putOp    int = 1
	appendOp int = 2

	// interval between each peek to pendingApply
	peekInterval time.Duration = 30 * time.Millisecond
	maxRetry     int           = 10
)

// Op would be the type of `Command` in `ApplyMsg`
type Op struct {
	OpType int
	Key    string
	Value  string
	OpID   int64 // given by GetArgs/PutAppendArgs
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db            map[string]string
	applyMsgs     OpID2ApplyMsg    // OpId - ApplyMsg. ApplyMsgs received on applyCh
	appliedResult map[int64]string // OpId - result from application, to avoid duplication
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{OpType: getOp, Key: args.Key, OpID: args.OpID}
	_, currTerm, isLeader := kv.rf.Start(op) // No lock needed as rf.Start() can handle concurrent Start()

	// not leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// is leader, wait for reply on applyCh
	for retry := maxRetry; retry > 0; retry-- {
		kv.mu.Lock()
		if _, ok := kv.appliedResult[args.OpID]; ok {
			if currTerm == kv.applyMsgs.Get(args.OpID).CommandTerm {
				reply.Value = kv.appliedResult[args.OpID]
				if reply.Value == "" {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
				}
			} else { // lose leadership before the request is committed
				reply.Err = ErrWrongLeader
			}
			defer kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(peekInterval)
	}

	reply.Err = ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// construct op
	var opType int
	if args.Op == "Put" {
		opType = putOp
	} else if args.Op == "Append" {
		opType = appendOp
	}
	op := Op{OpType: opType, Key: args.Key, Value: args.Value, OpID: args.OpID}
	_, currTerm, isLeader := kv.rf.Start(op)

	// not leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// is leader, wait for reply on applyCh
	for retry := maxRetry; retry > 0; retry-- {
		kv.mu.Lock()
		if _, ok := kv.appliedResult[args.OpID]; ok {
			if currTerm == kv.applyMsgs.Get(args.OpID).CommandTerm {
				reply.Err = OK
			} else { // lose leadership before the request is committed
				reply.Err = ErrWrongLeader
			}
			defer kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(peekInterval)
	}

	reply.Err = ErrWrongLeader
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func OpStr(op Op) string {
	var opTypeStr string
	var kvStr string
	if op.OpType == getOp {
		opTypeStr = "get"
		kvStr = fmt.Sprintf("\"%v\"", op.Key)
	} else if op.OpType == putOp {
		opTypeStr = "put"
		kvStr = fmt.Sprintf("\"%v\", \"%v\"", op.Key, op.Value)
	} else {
		opTypeStr = "append"
		kvStr = fmt.Sprintf("\"%v\", \"%v\"", op.Key, op.Value)
	}

	return fmt.Sprintf("%v: %v(%v)", op.OpID, opTypeStr, kvStr)
}

func (kv *KVServer) keepReadingApplyCh() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op) // We know we can do the cast

			kv.mu.Lock()
			if _, ok := kv.appliedResult[op.OpID]; ok {
				kv.mu.Unlock()
				continue
			}

			// Execute the command
			switch op.OpType {
			case getOp:
				v := kv.dbGet(op.Key)
				kv.appliedResult[op.OpID] = v
			case putOp:
				kv.dbPut(op.Key, op.Value)
				kv.appliedResult[op.OpID] = ""
			case appendOp:
				kv.dbAppend(op.Key, op.Value)
				kv.appliedResult[op.OpID] = ""
			}
			// DPrintf("[%v] Applied %v", kv.me, OpStr(op))

			// Add to kv.applyMsgs
			kv.applyMsgs.Add(op.OpID, applyMsg)
			kv.mu.Unlock()
		}
	}
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.applyMsgs = OpID2ApplyMsg{m: make(map[int64]raft.ApplyMsg)}
	kv.appliedResult = make(map[int64]string)

	// A separate go routine for receiving on kv.applyCh
	go kv.keepReadingApplyCh()

	return kv
}
