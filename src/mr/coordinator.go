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

const ( 
	TaskIdle = iota
	TaskInProgress
	TaskCompleted
)

type Task struct { 
	TaskID    int
	Type      TaskType
	FileName  string
	State     int
	StartTime time.Time
}
type Coordinator struct {
	mu          sync.Mutex
	files       []string
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
	phase       TaskType 
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	//sockname：coordinator监听RPC请求所使用的Unix socket路径
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		phase:   MapTask,
	}
	c.mapTasks = make([]Task, len(files))
	for i, file := range files {
		c.mapTasks[i] = Task{
			TaskID:   i,
			Type:     MapTask,
			FileName: file,
			State:    TaskIdle,
		}
	}
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			TaskID: i,
			Type:   ReduceTask,
			State:  TaskIdle,
		}
	}
	c.server(sockname)
	return &c
}
func allTasksCompleted(tasks []Task) bool {
	for _, task := range tasks {
		if task.State != TaskCompleted {
			return false
		}
	}
	return true
}
func (c *Coordinator) advancePhaseLocked() {
	if c.phase == MapTask && allTasksCompleted(c.mapTasks) {
		c.phase = ReduceTask
	}
	if c.phase == ReduceTask && allTasksCompleted(c.reduceTasks) {
		c.phase = ExitTask
	}
}
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advancePhaseLocked()
	switch c.phase {
	case MapTask:
		return c.assignMapTask(reply)
	case ReduceTask:
		return c.assignReduceTask(reply)
	case ExitTask:
		reply.Type = ExitTask
		return nil
	}
	return nil
}
func (c *Coordinator) assignMapTask(reply *TaskReply) error {
	now := time.Now()
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.State == TaskInProgress && now.Sub(task.StartTime) > 10*time.Second {
			task.State = TaskIdle
		}
		if task.State == TaskIdle {
			task.State = TaskInProgress
			task.StartTime = now
			reply.Type = MapTask
			reply.TaskID = task.TaskID
			reply.FileName = task.FileName
			reply.NMap = len(c.mapTasks)
			reply.NReduce = c.nReduce
			return nil
		}
	}
	c.advancePhaseLocked()
	if c.phase == ReduceTask {
		return c.assignReduceTask(reply)
	}
	if c.phase == ExitTask {
		reply.Type = ExitTask
		return nil
	}
	reply.Type = WaitTask
	return nil
}
func (c *Coordinator) assignReduceTask(reply *TaskReply) error { 
	now := time.Now()
	for i := range c.reduceTasks {
		task := &c.reduceTasks[i]
		if task.State == TaskInProgress && now.Sub(task.StartTime) > 10*time.Second {
			task.State = TaskIdle
		}
		if task.State == TaskIdle {
			task.State = TaskInProgress
			task.StartTime = now
			reply.Type = ReduceTask
			reply.TaskID = task.TaskID
			reply.NMap = len(c.mapTasks)
			reply.NReduce = c.nReduce
			return nil
		}
	}
	c.advancePhaseLocked()
	if c.phase == ExitTask {
		reply.Type = ExitTask
		return nil
	}
	reply.Type = WaitTask
	return nil
}
func (c *Coordinator) ReportTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var tasks []Task
	switch args.Type {
	case MapTask:
		tasks = c.mapTasks
	case ReduceTask:
		tasks = c.reduceTasks
	default:
		return nil
	}
	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}
	task := &tasks[args.TaskID]
	if task.State == TaskInProgress {
		task.State = TaskCompleted
	}
	c.advancePhaseLocked()
	return nil
}
func (c *Coordinator) Done() bool { 
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advancePhaseLocked()
	return c.phase == ExitTask
}
