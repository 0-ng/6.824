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
			//fmt.Println("receive", reply.FileName)
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))
			fc := map[string]*os.File{}
			keyFiles := make([]struct {
				Key      string
				FileName string
			}, 0)
			for _, kv := range kva {
				oname := fmt.Sprintf("mr-%v-%v", ihash(reply.FileName), ihash(kv.Key))
				if _, ok := fc[oname]; !ok {
					//fmt.Println("open", oname)
					ofile, _ := os.Create(oname)
					fc[oname] = ofile
					keyFiles = append(keyFiles, struct {
						Key      string
						FileName string
					}{
						Key:      kv.Key,
						FileName: oname,
					})
				}
				enc := json.NewEncoder(fc[oname])
				_ = enc.Encode(&kv)
			}
			for _, o := range fc {
				_ = o.Close()
			}

			doneArgs := DoneTaskArgs{
				FileName: reply.FileName,
				KeyFiles: keyFiles,
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
			var ret KeyValue
			ret.Key = reply.ReduceKey
			kvs := []string{}
			for _, fileName := range reply.ReduceKeyFiles {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvs = append(kvs, kv.Value)
				}
				file.Close()
			}
			ret.Value = reducef(ret.Key, kvs)
			oname := fmt.Sprintf("mr-out-%v", ihash(ret.Key))
			ofile, _ := os.Create(oname)
			fmt.Fprintf(ofile, "%v %v\n", ret.Key, ret.Value)
			ofile.Close()

			doneArgs := DoneTaskArgs{
				ReduceKey: ret.Key,
				TaskType:  Reduce,
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
