package raft

import (
	"log"
)

// Debugging
const Debug = true

//const Debug = false

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
