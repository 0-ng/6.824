package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	args := GetTaskCntArgs{}
	reply := GetTaskCntReply{}
	ok := call("Coordinator.GetTaskCnt", &args, &reply)
	if !ok {
		return
	}
	NReduce := reply.NReduce
	NMap := reply.NMap

	// uncomment to send the Example RPC to the coordinator.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			break
		}
		switch reply.TaskType {
		case Waiting:
			time.Sleep(time.Second)
		case Map:
			//fmt.Println("receive", reply.MapTask)
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			fc := map[string]*os.File{}
			for i := 0; i < NReduce; i++ {
				oname := fmt.Sprintf("mr-%v-%v", reply.MapTask.TaskID, i)
				ofile, _ := os.Create(oname)
				fc[oname] = ofile
			}

			kva := mapf(reply.FileName, string(content))
			for _, kv := range kva {
				oname := fmt.Sprintf("mr-%v-%v", reply.MapTask.TaskID, ihash(kv.Key)%NReduce)
				enc := json.NewEncoder(fc[oname])
				_ = enc.Encode(&kv)
			}
			for _, o := range fc {
				_ = o.Close()
			}

			doneArgs := DoneTaskArgs{
				MapTask:  reply.MapTask,
				TaskType: Map,
			}

			doneReply := DoneTaskReply{}
			for i := 0; i < 3; i++ {
				ok = call("Coordinator.DoneTask", &doneArgs, &doneReply)
				if !ok {
					fmt.Printf("call failed!\n")
					time.Sleep(time.Second)
					continue
				}
				break
			}

		case Reduce:
			vlist := make(ByKey, 0)
			for i := 0; i < NMap; i++ {
				oname := fmt.Sprintf("mr-%v-%v", i, reply.ReduceTask.TaskID)
				ofile, _ := os.Open(oname)
				dec := json.NewDecoder(ofile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					vlist = append(vlist, kv)
				}
				_ = ofile.Close()
			}
			sort.Sort(vlist)

			vvKey := ""
			vvlist := make([]string, 0)

			oname := fmt.Sprintf("mr-out-%v", reply.ReduceTask.TaskID)
			ofile, _ := os.Create(oname)

			for i := range vlist {
				if vvKey == vlist[i].Key || vvKey == "" {
					vvKey = vlist[i].Key
					vvlist = append(vvlist, vlist[i].Value)
					continue
				}
				value := reducef(vvKey, vvlist)
				fmt.Fprintf(ofile, "%v %v\n", vvKey, value)

				vvKey = vlist[i].Key
				vvlist = []string{vlist[i].Value}
			}
			if len(vvlist) != 0 {
				value := reducef(vvKey, vvlist)
				fmt.Fprintf(ofile, "%v %v\n", vvKey, value)
			}

			ofile.Close()

			doneArgs := DoneTaskArgs{
				ReduceTask: reply.ReduceTask,
				TaskType:   Reduce,
			}
			doneReply := DoneTaskReply{}
			for i := 0; i < 3; i++ {
				ok = call("Coordinator.DoneTask", &doneArgs, &doneReply)
				if !ok {
					fmt.Printf("call failed!\n")
					time.Sleep(time.Second)
					continue
				}
				break
			}
		case Done:
			return
		}
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Run(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, filename string) []struct {
	Key      string
	FileName string
} {

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	ret := make([]struct {
		Key      string
		FileName string
	}, 0)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		oname := fmt.Sprintf("mr-%v-%v", ihash(filename), ihash(intermediate[i].Key))
		ofile, _ := os.Create(oname)
		// this is the correct format for each line of Reduce output.
		//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		enc := json.NewEncoder(ofile)
		_ = enc.Encode(&KeyValue{Key: intermediate[i].Key, Value: output})

		ofile.Close()

		ret = append(ret,
			struct {
				Key      string
				FileName string
			}{Key: intermediate[i].Key, FileName: oname},
		)
		i = j
	}
	return ret
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
		//log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
