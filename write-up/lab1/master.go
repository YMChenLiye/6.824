package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	files          []string      // 原始文件
	nReduce        int           // reduce任务数
	todoMapTask    map[int]int64 // 未完成的Map任务,key:taskID, value:taskPatchTime
	todoReduceTask map[int]int64 // 未完成的Reduce任务,key:taskID, value:taskPatchTime
	mutex          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(req *GetTaskReq, rsp *GetTaskRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for k := range m.todoMapTask {
		if m.todoMapTask[k] == 0 {
			rsp.Status = "Task"
			rsp.Filename = m.files[k]
			rsp.NReduce = m.nReduce
			rsp.TaskID = k
			m.todoMapTask[k] = time.Now().Unix()
			return nil
		}
	}
	if len(m.todoMapTask) != 0 {
		rsp.Status = "Wait"
		return nil
	}

	for k := range m.todoReduceTask {
		if m.todoReduceTask[k] == 0 {
			rsp.Status = "Task"
			rsp.NReduce = m.nReduce
			rsp.NMap = len(m.files)
			rsp.TaskID = k
			m.todoReduceTask[k] = time.Now().Unix()
			return nil
		}
	}

	if len(m.todoReduceTask) != 0 {
		rsp.Status = "Wait"
		return nil
	} else {
		rsp.Status = "Exit"
		return nil
	}

	return nil
}

func (m *Master) FinishTask(req *FinishTaskReq, rsp *FinishTaskRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if req.BMapTask {
		fmt.Printf("Finish MapTask, TaskID:%d, cost time:%d\n", req.TaskID, time.Now().Unix()-m.todoMapTask[req.TaskID])
		delete(m.todoMapTask, req.TaskID)
	} else {
		fmt.Printf("Finish ReduceTask, TaskID:%d, cost time:%d\n", req.TaskID, time.Now().Unix()-m.todoReduceTask[req.TaskID])
		delete(m.todoReduceTask, req.TaskID)
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// clear timeout task
	for i, taskPatchTime := range m.todoMapTask {
		if taskPatchTime != 0 && taskPatchTime+10 < time.Now().Unix() {
			fmt.Printf("MapTask Timeout: TaskID:%d\n", i)
			m.todoMapTask[i] = 0
		}
	}
	for i, taskPatchTime := range m.todoReduceTask {
		if taskPatchTime != 0 && taskPatchTime+10 < time.Now().Unix() {
			fmt.Printf("ReduceTask Timeout: TaskID:%d\n", i)
			m.todoReduceTask[i] = 0
		}
	}

	ret = len(m.todoMapTask) == 0 && len(m.todoReduceTask) == 0

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.nReduce = nReduce

	m.todoMapTask = make(map[int]int64)
	for i, _ := range m.files {
		m.todoMapTask[i] = 0
	}

	m.todoReduceTask = make(map[int]int64)

	for i := 0; i < nReduce; i++ {
		m.todoReduceTask[i] = 0
	}

	// 清理其他任务的中间文件
	ClearDirtyFile()

	m.server()
	return &m
}

func ClearDirtyFile() {
	directory, _ := os.Getwd()
	// Open the directory and read all its files.
	dirRead, _ := os.Open(directory)
	dirFiles, _ := dirRead.Readdir(0)

	// Loop over the directory's files.
	for index := range dirFiles {
		fileHere := dirFiles[index]

		// Get name of file and its full path.
		nameHere := fileHere.Name()
		fullPath := directory + "/" + nameHere
		if strings.Index(nameHere, "mr-intermediate-") == 0 || strings.Index(nameHere, "mr-out-") == 0 {
			// Remove the file.
			os.Remove(fullPath)
			//fmt.Println("Removed file:", fullPath)
		}
	}
}
