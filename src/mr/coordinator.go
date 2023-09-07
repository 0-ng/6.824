package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	NReduce int

	MapFiles        []string
	MapRunningFiles []struct {
		FileName  string
		BeginTime time.Time
	}

	ReduceKeyMap      map[string][]string
	ReduceKeys        []string
	ReduceRunningKeys []struct {
		Key       string
		BeginTime time.Time
	}

	Mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if len(c.MapFiles) != 0 {
		reply.FileName = c.MapFiles[0]
		reply.TaskType = Map
		c.MapFiles = c.MapFiles[1:]
		c.MapRunningFiles = append(c.MapRunningFiles, struct {
			FileName  string
			BeginTime time.Time
		}{
			FileName:  reply.FileName,
			BeginTime: time.Now(),
		})
		return nil
	}
	if len(c.MapRunningFiles) != 0 {
		reply.TaskType = Waiting
		return nil
	}

	if len(c.ReduceKeys) != 0 {
		reply.ReduceKey = c.ReduceKeys[0]
		reply.TaskType = Reduce
		reply.ReduceKeyFiles = c.ReduceKeyMap[reply.ReduceKey]
		c.ReduceKeys = c.ReduceKeys[1:]
		c.ReduceRunningKeys = append(c.ReduceRunningKeys, struct {
			Key       string
			BeginTime time.Time
		}{
			Key:       reply.ReduceKey,
			BeginTime: time.Now(),
		})
		return nil
	}
	if len(c.ReduceRunningKeys) != 0 {
		reply.TaskType = Waiting
		return nil
	}

	reply.TaskType = Done
	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	switch args.TaskType {
	case Map:
		for _, file := range args.KeyFiles {
			if _, ok := c.ReduceKeyMap[file.Key]; !ok {
				c.ReduceKeyMap[file.Key] = make([]string, 0)
			}
			c.ReduceKeyMap[file.Key] = append(c.ReduceKeyMap[file.Key], file.FileName)
			c.ReduceKeys = append(c.ReduceKeys, file.Key)
		}
		for i := range c.MapRunningFiles {
			if c.MapRunningFiles[i].FileName == args.FileName {
				c.MapRunningFiles = append(c.MapRunningFiles[:i], c.MapRunningFiles[i+1:]...)
				break
			}
		}
	case Reduce:
		for i := range c.ReduceRunningKeys {
			if c.ReduceRunningKeys[i].Key == args.ReduceKey {
				c.ReduceRunningKeys = append(c.ReduceRunningKeys[:i], c.ReduceRunningKeys[i+1:]...)
				break
			}
		}

	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	//fmt.Println("still has ", len(c.MapFiles), len(c.MapRunningFiles),
	//	len(c.ReduceKeys), len(c.ReduceRunningKeys))
	return len(c.MapFiles) == 0 && len(c.MapRunningFiles) == 0 &&
		len(c.ReduceKeys) == 0 && len(c.ReduceRunningKeys) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapFiles:     files,
		NReduce:      nReduce,
		ReduceKeyMap: map[string][]string{},
		ReduceKeys:   []string{},
	}

	// Your code here.
	go c.checkRunningTask()
	//fmt.Printf("start with %d files\n", len(files))

	c.server()
	return &c
}

func (c *Coordinator) checkRunningTask() {
	for {
		time.Sleep(time.Second)
		c.Mutex.Lock()
		newMapRunningFiles := make([]struct {
			FileName  string
			BeginTime time.Time
		}, 0)

		newReduceRunningKeys := make([]struct {
			Key       string
			BeginTime time.Time
		}, 0)

		for i := range c.MapRunningFiles {
			if time.Now().Sub(c.MapRunningFiles[i].BeginTime) < 10*time.Second {
				newMapRunningFiles = append(newMapRunningFiles, c.MapRunningFiles[i])
			} else {
				c.MapFiles = append(c.MapFiles, c.MapRunningFiles[i].FileName)
			}
		}
		for i := range c.ReduceRunningKeys {
			if time.Now().Sub(c.ReduceRunningKeys[i].BeginTime) < 10*time.Second {
				newReduceRunningKeys = append(newReduceRunningKeys, c.ReduceRunningKeys[i])
			} else {
				c.ReduceKeys = append(c.ReduceKeys, c.ReduceRunningKeys[i].Key)
			}
		}
		c.MapRunningFiles = newMapRunningFiles
		c.ReduceRunningKeys = newReduceRunningKeys
		c.Mutex.Unlock()
	}
}
