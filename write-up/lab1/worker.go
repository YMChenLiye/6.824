package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		time.Sleep(time.Second)

		req := GetTaskReq{}
		req.No = 1
		rsp := GetTaskRsp{}
		ok := call("Master.GetTask", &req, &rsp)
		if ok {
			fmt.Println(rsp.Status, rsp.TaskID, len(rsp.Filename) > 0)
			if rsp.Status == "Wait" {
				// do nothing
			} else if rsp.Status == "Task" {
				doTask(&req, &rsp, mapf, reducef)
			} else if rsp.Status == "Exit" {
				break
			} else {
				fmt.Printf("unknow status\n")
			}
		} else {
			fmt.Println("rpc error")
		}

	}
	// uncomment to send the Example RPC to the master.
	CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doTask(req *GetTaskReq, rsp *GetTaskRsp, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	if len(rsp.Filename) > 0 {
		// do map task
		//intermediate := []mr.KeyValue{}
		intermediate := make(map[int][]KeyValue)
		file, err := os.Open(rsp.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", rsp.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", rsp.Filename)
		}
		file.Close()
		kva := mapf(rsp.Filename, string(content))
		for _, v := range kva {
			index := ihash(v.Key) % rsp.NReduce
			intermediate[index] = append(intermediate[index], v)
		}

		// 写文件
		for index, v := range intermediate {
			filename := "mr-intermediate-" + strconv.Itoa(rsp.TaskID) + "-" + strconv.Itoa(index)
			tmpfile, err := ioutil.TempFile("", "tmp")
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(tmpfile)
			for _, kv := range v {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write file %s", tmpfile.Name())
				}
			}
			os.Rename(tmpfile.Name(), filename)
		}

	} else {
		// do reduce task
		intermediate := []KeyValue{}
		for i := 0; i < rsp.NMap; i++ {
			filename := "mr-intermediate-" + strconv.Itoa(i) + "-" + strconv.Itoa(rsp.TaskID)
			file, err := os.Open(filename)
			if err != nil {
				// 没有这个文件,跳过
				fmt.Printf("cannot open file:%s, maybe key too few\n", filename)
				continue
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}

		// 输出
		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + strconv.Itoa(rsp.TaskID)
		tmpfile, err := ioutil.TempFile("", "tmp")
		if err != nil {
			log.Fatal(err)
		}
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

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		os.Rename(tmpfile.Name(), oname)
	}

	// Task Finish
	call("Master.FinishTask", &FinishTaskReq{len(rsp.Filename) > 0, rsp.TaskID}, &FinishTaskRsp{})
	return true
}
