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
	mp                           map[string]string
	clientPutAppendRequest       map[string]PutAppendReply
	clientPutAppendRequestStatus map[string]ProcessStatus
	clientGetRequest             map[string]GetReply
	clientGetRequestStatus       map[string]ProcessStatus
	clientRequestChannel         map[string]chan struct{}
}

func (kv *KVServer) Lock() {
	DPrintf("[lock] %v lock", kv.me)
	kv.mu.L.Lock()
	DPrintf("[lock] %v lock success", kv.me)
}
func (kv *KVServer) Unlock() {
	DPrintf("[unlock] %v unlock", kv.me)
	kv.mu.L.Unlock()
	DPrintf("[unlock] %v unlock success", kv.me)
}

func (kv *KVServer) Wait() {
	kv.mu.Wait()
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
	requestID := args.RequestID
	start := time.Now()
	defer func() {
		DPrintf("[KVServer.Get] %v me=%v, args[%+v], reply[%+v], cost[%v]\n", requestID, kv.me, args, reply, time.Since(start))
	}()
	// Your code here.
	kv.Lock()
	// 幂等
	if status := kv.clientGetRequestStatus[requestID]; status != ProcessStatusNone {
		for kv.clientGetRequestStatus[requestID] == ProcessStatusProcessing {
			if time.Since(start) > time.Second {
				kv.clientGetRequestStatus[requestID] = ProcessStatusNone
				reply.Err = ErrTimeout
				kv.Unlock()
				return
			}
			DPrintf("[KVServer.Get] %v me=%v, waiting %v\n", requestID, kv.me, args.RequestID)
			kv.Wait()
		}
		if kv.clientGetRequestStatus[args.RequestID] == ProcessStatusDone {
			reply.Value = kv.clientGetRequest[args.RequestID].Value
			reply.Err = kv.clientGetRequest[args.RequestID].Err
			kv.Unlock()
			return
		}
	}

	op := Op{
		RequestID: args.RequestID,
		Key:       args.Key,
		Op:        OpGet,
		From:      kv.me,
	}
	// 写日志
	logID, termID, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("[KVServer.Get] %v me=%v, starting %v, is not leader\n", requestID, kv.me, op.RequestID)
		reply.Err = ErrWrongLeader
		kv.Unlock()
		return
	}

	DPrintf("[KVServer.Get] %v me=%v, starting idx=%v termID=%v\n", requestID, kv.me, logID, termID)
	kv.clientGetRequestStatus[args.RequestID] = ProcessStatusProcessing
	// 等结果
	for kv.clientGetRequestStatus[args.RequestID] == ProcessStatusProcessing {
		if time.Since(start) > time.Second {
			kv.clientGetRequestStatus[args.RequestID] = ProcessStatusNone
			reply.Err = ErrTimeout
			kv.Unlock()
			return
		}
		DPrintf("[KVServer.Get] %v me=%v, waiting %v\n", requestID, kv.me, args.RequestID)
		kv.Wait()
	}
	reply.Value = kv.clientGetRequest[args.RequestID].Value
	reply.Err = kv.clientGetRequest[args.RequestID].Err
	kv.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	requestID := args.RequestID
	start := time.Now()
	defer func() {
		DPrintf("[KVServer.PutAppend] %v me=%v, args[%+v], reply[%+v], cost[%v]\n", requestID, kv.me, args, reply, time.Since(start))
	}()
	if args.Op != OpPUT && args.Op != OpAppend {
		reply.Err = "TODO"
		return
	}
	// Your code here.
	kv.Lock()
	// 幂等
	if status := kv.clientPutAppendRequestStatus[args.RequestID]; status != ProcessStatusNone {
		for kv.clientPutAppendRequestStatus[args.RequestID] == ProcessStatusProcessing {
			if time.Since(start) > time.Second {
				kv.clientPutAppendRequestStatus[args.RequestID] = ProcessStatusNone
				reply.Err = ErrTimeout
				kv.Unlock()
				return
			}
			DPrintf("[KVServer.PutAppend] %v me=%v, waiting %v\n", requestID, kv.me, args.RequestID)
			kv.Wait()
		}
		reply.Err = kv.clientPutAppendRequest[args.RequestID].Err
		kv.Unlock()
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

	logID, termID, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("[KVServer.PutAppend] %v me=%v, starting %v, is not leader\n", requestID, kv.me, op.RequestID)
		reply.Err = ErrWrongLeader
		kv.Unlock()
		return
	}

	DPrintf("[KVServer.PutAppend] %v me=%v, starting idx=%v termID=%v\n", requestID, kv.me, logID, termID)
	kv.clientPutAppendRequestStatus[args.RequestID] = ProcessStatusProcessing
	for kv.clientPutAppendRequestStatus[args.RequestID] == ProcessStatusProcessing {
		if time.Since(start) > time.Second {
			kv.clientPutAppendRequestStatus[args.RequestID] = ProcessStatusNone
			reply.Err = ErrTimeout
			kv.Unlock()
			return
		}
		DPrintf("[KVServer.PutAppend] %v me=%v, waiting %v\n", requestID, kv.me, args.RequestID)
		kv.Wait()
	}
	reply.Err = kv.clientPutAppendRequest[args.RequestID].Err
	kv.Unlock()
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
	kv.clientGetRequestStatus = make(map[string]ProcessStatus)
	kv.clientPutAppendRequestStatus = make(map[string]ProcessStatus)

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
					kv.clientPutAppendRequestStatus[o.RequestID] = ProcessStatusDone
				}
			case OpAppend:
				if _, ok := kv.clientPutAppendRequest[o.RequestID]; !ok {
					kv.mp[o.Key] += o.Value
					kv.clientPutAppendRequest[o.RequestID] = PutAppendReply{
						Err: OK,
					}
				}
				kv.clientPutAppendRequestStatus[o.RequestID] = ProcessStatusDone
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
					kv.clientGetRequestStatus[o.RequestID] = ProcessStatusDone
				}
			}
			kv.Unlock()
			kv.mu.Broadcast()

			DPrintf("[kvserver.ApplyLog]%v apply %+v done\n", kv.me, o)
		}
	}
}
