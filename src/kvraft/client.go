package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	rand2 "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	n         int
	term      int
	leaderID  int
	mu        sync.Mutex
	requestID uint64
	me        int
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
	ck.me = rand2.Int()
	time.Sleep(time.Millisecond * 100)
	//ck.reloadLeader()
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
func (ck *Clerk) Get(key string) (val string) {
	start := time.Now()
	defer func() {
		DPrintf("[Clerk.Get] get key[%v] val[%v], cost[%v]\n", key, val, time.Since(start))
	}()
	// You will have to modify this function.
	args := &GetArgs{Key: key, RequestID: ck.GenerateRequestID()}
	reply := &GetReply{}
	ld := ck.getLeader()
	for {
		for i := 0; i < ck.n; i++ {
			server := ck.servers[(ld+i)%ck.n]

			ch := make(chan bool)
			go func() {
				ch <- server.Call("KVServer.Get", args, reply)
			}()
			select {
			case ok := <-ch:
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.setLeader((ld + i) % ck.n)
					return reply.Value
				}
			case <-time.Tick(time.Second):

			}

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
	start := time.Now()
	defer func() {
		DPrintf("[Clerk.PutAppend] %v key[%v]=value[%v], cost[%v]\n", op, key, value, time.Since(start))
	}()
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Op: op, Value: value, RequestID: ck.GenerateRequestID()}
	reply := &PutAppendReply{}
	ld := ck.getLeader()
	for {
		for i := 0; i < ck.n; i++ {
			server := ck.servers[(ld+i)%ck.n]
			ch := make(chan bool)
			go func() {
				ch <- server.Call("KVServer.PutAppend", args, reply)
			}()
			select {
			case ok := <-ch:
				if ok && reply.Err == OK {
					ck.setLeader((ld + i) % ck.n)
					return
				}
			case <-time.Tick(time.Second):

			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GenerateRequestID() (requestID string) {
	id := atomic.AddUint64(&ck.requestID, 1)
	return fmt.Sprintf("%v:%v", ck.me, id)
}

func (ck *Clerk) getLeader() (leaderID int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leaderID
}

func (ck *Clerk) setLeader(leaderID int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderID = leaderID
}

//
//func (ck *Clerk) reloadLeader() {
//	for {
//		time.Sleep(time.Duration(50+rand2.Int()%10) * time.Millisecond)
//		ck.heartbeat()
//		if ck.term > 0 {
//			break
//		}
//	}
//	go func() {
//		for {
//			fmt.Println("?")
//			time.Sleep(time.Duration(50+rand2.Int()%10) * time.Millisecond)
//			ck.heartbeat()
//		}
//	}()
//}
//
//func (ck *Clerk) heartbeat() {
//	//wg := sync.WaitGroup{}
//	for _, server := range ck.servers {
//
//		//args := &PutAppendArgs{Key: "key", Op: "Put", Value: "value"}
//		//reply := &PutAppendReply{}
//		//ok := server.Call("KVServer.PutAppend", args, reply)
//		//if ok && reply.Err == OK {
//		//	ck.leaderID = id
//		//	ck.term = 1
//		//	return
//		//}
//		ck.Get("?")
//
//		//wg.Add(1)
//		//go func(server *labrpc.ClientEnd) {
//		//	defer wg.Done()
//		args := &GetLeaderArgs{}
//		reply := &GetLeaderReply{}
//		ok := server.Call("KVServer.GetLeader", args, reply)
//		fmt.Println(ok, args, reply)
//		if !ok {
//			return
//		}
//		//ck.mu.Lock()
//		//defer ck.mu.Unlock()
//		if reply.Term > ck.term {
//			ck.leaderID = reply.LeaderID
//			ck.term = reply.Term
//		}
//		//}(ck.servers[i])
//	}
//	//wg.Wait()
//}
