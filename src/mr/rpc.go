package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.

type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}
type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	WaitTask   TaskType = "wait"
	ExitTask   TaskType = "exit"
)

type TaskArgs struct {
}
type TaskReply struct {
	Type     TaskType
	TaskID   int
	FileName string
	NMap     int
	NReduce  int
}
type TaskFinishedArgs struct {
	Type   TaskType
	TaskID int
}
type TaskFinishedReply struct {
}

// Add your RPC definitions here.
