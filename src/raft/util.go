package raft

import (
	"log"
	"runtime"
)

const Debug = 0

func trace() {
	pc := make([]uintptr, 15)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	log.Printf("%s:%d %s\n", frame.File, frame.Line, frame.Function)
}

func DPrintf(format string, a ...interface{}) {
	if Debug >= 1 {
		log.Printf(format, a...)
	}
	return
}

func min(a int, b int) int {
	if a <= b {
		return a
	}

	return b
}
