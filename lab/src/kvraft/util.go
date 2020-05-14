package kvraft

import (
	"log"

	"../raft"
)

// Printing utility
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// OpID2ApplyMsg used by server.go
type OpID2ApplyMsg struct {
	m map[int64]raft.ApplyMsg
}

func (m *OpID2ApplyMsg) Contains(key int64) bool {
	_, ok := m.m[key]
	return ok
}

func (m *OpID2ApplyMsg) Add(key int64, msg raft.ApplyMsg) {
	m.m[key] = msg
}

func (m *OpID2ApplyMsg) Remove(key int64) {
	_, ok := m.m[key]
	if ok {
		delete(m.m, key)
	}
}

func (m *OpID2ApplyMsg) Get(key int64) raft.ApplyMsg {
	return m.m[key]
}

// private functions used by KVServer to execute Op when the
// operation is committed by raft
func (kv *KVServer) dbGet(key string) string {
	value, ok := kv.db[key]
	if ok {
		return value
	} else {
		return ""
	}
}

func (kv *KVServer) dbPut(key string, value string) {
	kv.db[key] = value
}

func (kv *KVServer) dbAppend(key string, value string) {
	_, ok := kv.db[key]
	if !ok {
		kv.db[key] = ""
	}
	kv.db[key] += value
}
