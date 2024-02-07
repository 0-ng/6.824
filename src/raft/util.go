package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

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

const TDebug = true

//const TDebug = false

func TDPrintf(format string, a ...interface{}) (n int, err error) {
	if TDebug {
		log.Printf(format, a...)
	}
	return
}

func init() {
	f, err := os.OpenFile("a.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		log.Panic("打开日志文件异常")
	}
	//if f.Truncate(0) != nil {
	//	log.Panic("刪除日志文件异常")
	//}
	log.SetOutput(f)
}
