package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		//fmt.Printf(format, a...)
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestID string `json:"request_id"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Op        string `json:"op,omitempty"`
	From      int    `json:"from"`
}

type KVServer struct {
	//mu      sync.Mutex
	mu      *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mp                     map[string]string
	clientPutAppendRequest map[string]PutAppendReply
	clientGetRequest       map[string]GetReply
	clientRequestChannel   map[string]chan struct{}
}

func (kv *KVServer) Lock() {
	kv.mu.L.Lock()
}
func (kv *KVServer) Unlock() {
	kv.mu.L.Unlock()
}

func (kv *KVServer) GetLeader(args *GetLeaderArgs, reply *GetLeaderReply) {
	defer func() {
		DPrintf("[KVServer.GetLeader] args[%+v], reply[%+v]\n", args, reply)
	}()
	kv.Lock()
	defer kv.Unlock()
	reply.LeaderID = kv.rf.LeaderID
	reply.Term = kv.rf.CurrentTerm
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	start := time.Now()
	defer func() {
		DPrintf("[KVServer.Get] me=%v, args[%+v], reply[%+v], cost[%v]\n", kv.me, args, reply, time.Since(start))
	}()
	// Your code here.
	kv.Lock()
	// 幂等
	if rsp, ok := kv.clientGetRequest[args.RequestID]; ok {
		kv.Unlock()
		reply.Value = rsp.Value
		reply.Err = rsp.Err
		return
	}
	ch := make(chan struct{}, 1)
	kv.clientRequestChannel[args.RequestID] = ch
	kv.Unlock()

	op := Op{
		RequestID: args.RequestID,
		Key:       args.Key,
		Op:        OpGet,
		From:      kv.me,
	}
	// 写日志
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等结果
	<-ch

	kv.Lock()
	defer kv.Unlock()
	rsp, ok := kv.clientGetRequest[args.RequestID]
	if !ok {
		// 不应该的样子
		panic("[KVServer.Get]")
	}
	reply.Value = rsp.Value
	reply.Err = rsp.Err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	start := time.Now()
	defer func() {
		DPrintf("[KVServer.PutAppend] me=%v, args[%+v], reply[%+v], cost[%v]\n", kv.me, args, reply, time.Since(start))
	}()
	if args.Op != OpPUT && args.Op != OpAppend {
		reply.Err = "TODO"
		return
	}
	// Your code here.
	kv.Lock()
	// 幂等
	if rsp, ok := kv.clientPutAppendRequest[args.RequestID]; ok {
		kv.Unlock()
		reply.Err = rsp.Err
		return
	}

	op := Op{
		RequestID: args.RequestID,
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		From:      kv.me,
	}
	// 写日志
	ch := make(chan struct{}, 1)
	kv.clientRequestChannel[args.RequestID] = ch
	kv.Unlock()

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	<-ch
	kv.Lock()
	defer kv.Unlock()
	//close(kv.clientRequestChannel[args.RequestID])
	//delete(kv.clientRequestChannel, args.RequestID)
	rsp, ok := kv.clientPutAppendRequest[args.RequestID]
	if !ok {
		// 不应该的样子
		panic("[KVServer.PutAppend]")
	}
	reply.Err = rsp.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.mu = sync.NewCond(&sync.Mutex{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientRequestChannel = make(map[string]chan struct{})
	kv.clientGetRequest = make(map[string]GetReply)
	kv.clientPutAppendRequest = make(map[string]PutAppendReply)

	// You may need initialization code here.
	go kv.applyLog()
	kv.mp = make(map[string]string)

	return kv
}
func (kv *KVServer) applyLog() {
	for !kv.killed() {
		select {
		case v := <-kv.applyCh:
			o, ok := v.Command.(Op)
			if !ok {
				continue
			}
			DPrintf("[kvserver.ApplyLog]%v begin to apply %+v\n", kv.me, o)
			kv.Lock()
			switch o.Op {
			case OpPUT:
				if _, ok := kv.clientPutAppendRequest[o.RequestID]; !ok {
					kv.mp[o.Key] = o.Value
					kv.clientPutAppendRequest[o.RequestID] = PutAppendReply{
						Err: OK,
					}
				}
			case OpAppend:
				if _, ok := kv.clientPutAppendRequest[o.RequestID]; !ok {
					kv.mp[o.Key] += o.Value
					kv.clientPutAppendRequest[o.RequestID] = PutAppendReply{
						Err: OK,
					}
				}
			case OpGet:
				if _, ok := kv.clientGetRequest[o.RequestID]; !ok {
					value, ok := kv.mp[o.Key]
					err := OK
					if !ok {
						err = ErrNoKey
					}
					kv.clientGetRequest[o.RequestID] = GetReply{
						Value: value,
						Err:   err,
					}
				}
			}
			if len(o.RequestID) > 0 {
				kv.clientRequestChannel[o.RequestID] <- struct{}{}
			}
			kv.Unlock()

			DPrintf("[kvserver.ApplyLog]%v apply %+v done\n", kv.me, o)
		}
	}
}
