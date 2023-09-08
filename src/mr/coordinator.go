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

type MapTask struct {
	FileName string
	TaskID   int
}

type ReduceTask struct {
	TaskID int
}

type Coordinator struct {
	// Your definitions here.
	NReduce int
	NMap    int

	MapTaskList        []MapTask
	MapRunningTaskList []struct {
		MapTask   MapTask
		BeginTime time.Time
	}

	//ReduceKeyMap      map[string][]string
	ReduceTaskList        []ReduceTask
	ReduceRunningTaskList []struct {
		ReduceTask ReduceTask
		BeginTime  time.Time
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
	if len(c.MapTaskList) != 0 {
		reply.MapTask = c.MapTaskList[0]
		reply.TaskType = Map
		c.MapTaskList = c.MapTaskList[1:]
		c.MapRunningTaskList = append(c.MapRunningTaskList, struct {
			MapTask   MapTask
			BeginTime time.Time
		}{
			MapTask:   reply.MapTask,
			BeginTime: time.Now(),
		})
		return nil
	}
	if len(c.MapRunningTaskList) != 0 {
		reply.TaskType = Waiting
		return nil
	}

	if len(c.ReduceTaskList) != 0 {
		reply.ReduceTask = c.ReduceTaskList[0]
		reply.TaskType = Reduce
		c.ReduceTaskList = c.ReduceTaskList[1:]
		c.ReduceRunningTaskList = append(c.ReduceRunningTaskList, struct {
			ReduceTask ReduceTask
			BeginTime  time.Time
		}{
			ReduceTask: reply.ReduceTask,
			BeginTime:  time.Now(),
		})
		return nil
	}
	if len(c.ReduceRunningTaskList) != 0 {
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
		for i := range c.MapRunningTaskList {
			if c.MapRunningTaskList[i].MapTask == args.MapTask {
				c.MapRunningTaskList = append(c.MapRunningTaskList[:i], c.MapRunningTaskList[i+1:]...)
				break
			}
		}
	case Reduce:
		for i := range c.ReduceRunningTaskList {
			if c.ReduceRunningTaskList[i].ReduceTask == args.ReduceTask {
				c.ReduceRunningTaskList = append(c.ReduceRunningTaskList[:i], c.ReduceRunningTaskList[i+1:]...)
				break
			}
		}

	}
	return nil
}

func (c *Coordinator) GetTaskCnt(args *GetTaskCntArgs, reply *GetTaskCntReply) error {
	reply.NReduce = c.NReduce
	reply.NMap = c.NMap
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
	//fmt.Println("still has ", len(c.MapTaskList), len(c.MapRunningTaskList),
	//	len(c.ReduceTaskList), len(c.ReduceRunningTaskList))
	return len(c.MapTaskList) == 0 && len(c.MapRunningTaskList) == 0 &&
		len(c.ReduceTaskList) == 0 && len(c.ReduceRunningTaskList) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:     nReduce,
		NMap:        len(files),
		MapTaskList: make([]MapTask, 0, len(files)),
		MapRunningTaskList: make([]struct {
			MapTask   MapTask
			BeginTime time.Time
		}, 0),
		ReduceTaskList: make([]ReduceTask, 0),
		ReduceRunningTaskList: make([]struct {
			ReduceTask ReduceTask
			BeginTime  time.Time
		}, 0),
	}
	for i := range files {
		c.MapTaskList = append(c.MapTaskList, MapTask{TaskID: i, FileName: files[i]})
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskList = append(c.ReduceTaskList, ReduceTask{TaskID: i})
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

		newMapRunningTaskList := make([]struct {
			MapTask   MapTask
			BeginTime time.Time
		}, 0)
		newReduceRunningTaskList := make([]struct {
			ReduceTask ReduceTask
			BeginTime  time.Time
		}, 0)

		for i := range c.MapRunningTaskList {
			if time.Now().Sub(c.MapRunningTaskList[i].BeginTime) < 10*time.Second {
				newMapRunningTaskList = append(newMapRunningTaskList, c.MapRunningTaskList[i])
			} else {
				c.MapTaskList = append(c.MapTaskList, c.MapRunningTaskList[i].MapTask)
			}
		}
		for i := range c.ReduceRunningTaskList {
			if time.Now().Sub(c.ReduceRunningTaskList[i].BeginTime) < 10*time.Second {
				newReduceRunningTaskList = append(newReduceRunningTaskList, c.ReduceRunningTaskList[i])
			} else {
				c.ReduceTaskList = append(c.ReduceTaskList, c.ReduceRunningTaskList[i].ReduceTask)
			}
		}
		c.MapRunningTaskList = newMapRunningTaskList
		c.ReduceRunningTaskList = newReduceRunningTaskList
		c.Mutex.Unlock()
	}
}
