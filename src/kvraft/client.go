package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
	rand2 "math/rand"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	n        int
	term     int
	leaderID int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.n = len(servers)
	ck.reloadLeader()
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
	DPrintf("[Clerk.Get] get key[%v]\n", key)
	// You will have to modify this function.
	for _, server := range ck.servers {
		args := &GetArgs{Key: key}
		reply := &GetReply{}
		ok := server.Call("KVServer.Get", args, reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
	}
	return ""
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
	DPrintf("[Clerk.PutAppend] %v key[%v]=value[%v]\n", op, key, value)
	// You will have to modify this function.
	for _, server := range ck.servers {
		args := &PutAppendArgs{Key: key, Op: op, Value: value}
		reply := &PutAppendReply{}
		ok := server.Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) reloadLeader() {
	for {
		time.Sleep(time.Duration(50+rand2.Int()%10) * time.Millisecond)
		ck.heartbeat()
		if ck.term > 0 {
			break
		}
	}
	go func() {
		for {
			time.Sleep(time.Duration(50+rand2.Int()%10) * time.Millisecond)
			ck.heartbeat()
		}
	}()
}

func (ck *Clerk) heartbeat() {
	//wg := sync.WaitGroup{}
	for _, server := range ck.servers {
		//wg.Add(1)
		//go func(server *labrpc.ClientEnd) {
		//	defer wg.Done()
		args := &GetLeaderArgs{}
		reply := &GetLeaderReply{}
		ok := server.Call("KVServer.GetLeader", args, reply)
		fmt.Println(ok, args, reply)
		if !ok {
			return
		}
		//ck.mu.Lock()
		//defer ck.mu.Unlock()
		if reply.Term > ck.term {
			ck.leaderID = reply.LeaderID
			ck.term = reply.Term
		}
		//}(ck.servers[i])
	}
	//wg.Wait()
}
