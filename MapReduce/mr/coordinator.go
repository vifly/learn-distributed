package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex

type WorkStatus int32

const (
	Todo  WorkStatus = 0
	Doing WorkStatus = 1
	Done  WorkStatus = 2
)

type TaskInfo struct {
	taskId          int
	inputFilePath   string
	outputFilePaths []string
	workType        WorkType
	execTime        int
	status          WorkStatus
}

type TaskStatusManager struct {
	taskInfos []TaskInfo
}

func NewTaskStatusManager(files []string, nReduce int) TaskStatusManager {
	taskStatusManager := TaskStatusManager{}
	for taskId, f := range files {
		taskStatusManager.taskInfos = append(taskStatusManager.taskInfos, TaskInfo{taskId, f, make([]string, 0), Map, 0, Todo})
	}
	return taskStatusManager
}

// Will mark return todo work to doing
func (tm *TaskStatusManager) GetTodoWork() *TaskInfo {
	mu.Lock()
	defer mu.Unlock()
	for i, task := range tm.taskInfos {
		if task.status == Todo {
			tm.taskInfos[i].status = Doing
			return &task
		}
	}
	return nil
}

func (tm *TaskStatusManager) SetDone(taskId int, outputFilePaths []string) {
	mu.Lock()
	defer mu.Unlock()
	for i, task := range tm.taskInfos {
		if task.taskId == taskId {
			tm.taskInfos[i].status = Done
			tm.taskInfos[i].outputFilePaths = outputFilePaths
			break
		}
	}
}

func (tm *TaskStatusManager) SetTodo(taskId int) {
	mu.Lock()
	defer mu.Unlock()
	for i, task := range tm.taskInfos {
		if task.taskId == taskId {
			tm.taskInfos[i].status = Todo
			tm.taskInfos[i].execTime = 0
			break
		}
	}
}

// Add x seconds for all doing tasks.
// If no doing task, it will do nothing.
func (tm *TaskStatusManager) ExecTimeAdd(time int) {
	mu.Lock()
	defer mu.Unlock()
	for i, task := range tm.taskInfos {
		if task.status == Doing {
			tm.taskInfos[i].execTime = task.execTime + time
		}
	}
}

func (tm *TaskStatusManager) GetTimeOutTasks(maxTime int) []TaskInfo {
	mu.Lock()
	defer mu.Unlock()
	tasks := make([]TaskInfo, 0)
	for _, task := range tm.taskInfos {
		if task.execTime >= maxTime {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (tm *TaskStatusManager) IsAllDone() bool {
	mu.Lock()
	defer mu.Unlock()
	for _, task := range tm.taskInfos {
		if task.status != Done {
			return false
		}
	}
	return true
}

func (tm *TaskStatusManager) GetWorkType() WorkType {
	mu.Lock()
	defer mu.Unlock()
	return tm.taskInfos[0].workType
}

func (tm *TaskStatusManager) AddTodoWorks(taskInfos []TaskInfo) {
	mu.Lock()
	defer mu.Unlock()
	tm.taskInfos = append(tm.taskInfos, taskInfos...)
}

func (tm *TaskStatusManager) ClearAllWorks() {
	mu.Lock()
	defer mu.Unlock()
	tm.taskInfos = make([]TaskInfo, 0)
}

type Coordinator struct {
	taskStatusManager TaskStatusManager
	nReduce           int
}

func (c *Coordinator) GetWork(args *NullArgs, reply *GetWorkReply) error {
	setReply := func(taskInfo *TaskInfo) {
		reply.FilePath = taskInfo.inputFilePath
		reply.TaskId = taskInfo.taskId
		reply.WorkType = taskInfo.workType
		reply.IsEnd = false
	}
	reply.NReduce = c.nReduce

	taskInfo := c.taskStatusManager.GetTodoWork()
	// log.Println("todo:", taskInfo)

	if taskInfo == nil {
		for {
			if c.taskStatusManager.IsAllDone() {
				if c.taskStatusManager.GetWorkType() == Map {
					c.AddReduceTask()
					taskInfo = c.taskStatusManager.GetTodoWork()
					setReply(taskInfo)
				} else {
					reply.IsEnd = true
				}
				break
			} else {
				taskInfo = c.taskStatusManager.GetTodoWork()
				if taskInfo != nil {
					setReply(taskInfo)
					break
				}
			}
			// wait for all tasks is done
			time.Sleep(time.Second)
		}
	} else {
		setReply(taskInfo)
	}
	return nil
}

func (c *Coordinator) WorkDone(args *WorkDoneArgs, reply *NullReply) error {
	// log.Println("reply", args)
	c.taskStatusManager.SetDone(args.TaskId, args.OutputFilePaths)
	return nil
}

// call this func after all Map tasks is done
func (c *Coordinator) AddReduceTask() {
	taskInfos := make([]TaskInfo, 0)
	for reduceTaskId := 0; reduceTaskId <= c.nReduce; reduceTaskId++ {
		taskInfos = append(taskInfos, TaskInfo{reduceTaskId, "", make([]string, 0), Reduce, 0, Todo})
	}
	c.taskStatusManager.ClearAllWorks()
	c.taskStatusManager.AddTodoWorks(taskInfos)
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if c.taskStatusManager.IsAllDone() && c.taskStatusManager.GetWorkType() == Reduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.taskStatusManager = NewTaskStatusManager(files, nReduce)
	c.nReduce = nReduce
	go func() {
		for {
			time.Sleep(time.Second)
			c.taskStatusManager.ExecTimeAdd(1)
			timeOutTasks := c.taskStatusManager.GetTimeOutTasks(10)
			// if len(timeOutTasks) != 0 {
			// 	log.Println(timeOutTasks)
			// }
			for _, task := range timeOutTasks {
				c.taskStatusManager.SetTodo(task.taskId)
			}
			// graceful exit
			if c.taskStatusManager.IsAllDone() && c.taskStatusManager.GetWorkType() == Reduce {
				break
			}
		}
	}()

	c.server()
	return &c
}
