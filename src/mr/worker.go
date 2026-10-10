package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
	"io"
	"sort"
)
type KeyValue struct {
	Key   string
	Value string
}
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string 
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		coordSockName = sockname
	for{
		args := TaskArgs{}
		reply:=TaskReply{}
		ok:=call("Coordinator.RequestTask",&args,&reply)
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		switch reply.Type{
			case MapTask:
				if executeMapTask(&reply,mapf){
					reportTaskFinished(MapTask,reply.TaskID)
				}
			case ReduceTask:
			if executeReduceTask(&reply,reducef){
				reportTaskFinished(ReduceTask,reply.TaskID)
			}
			case WaitTask:
			time.Sleep(time.Second)
			case ExitTask:
				return
			default:
				log.Printf("unknow task type")
				time.Sleep(time.Second)
		}
	}
}
func executeMapTask(task*TaskReply,mapf func(string,string)[]KeyValue)bool{
	if task.NReduce<=0{
		log.Printf("NRduce=%d for map task %d",task.NReduce,task.TaskID,)
		return false
	}
	content,err:=os.ReadFile(task.FileName)
	if err!=nil{
		log.Printf("filename:%q,failed:%v",task.FileName,err,)
		return false
	}
	tempFiles:=make([]*os.File,task.NReduce)
	tempNames:=make([]string,task.NReduce)
	encoders:=make([]*json.Encoder,task.NReduce)
	cleanup:=func(){
		for i,f:=range tempFiles{
			if f!=nil{
				_=f.Close()
			}
			if tempNames[i]!=""{
				_=os.Remove(tempNames[i])
			}
		}
	}
	for i:=0;i<task.NReduce;i++{
		f,err:=os.CreateTemp(".","mr-tmp-*")
		if err!=nil{
			log.Printf("create file failed:%v",err)
			cleanup()
			return false
		}
		tempFiles[i]=f
		tempNames[i]=f.Name()
		encoders[i]=json.NewEncoder(f)
	}
		kva:=mapf(task.FileName,string(content))
	for _,kv :=range kva{
		partition:=ihash(kv.Key)%task.NReduce
		if err:=encoders[partition].Encode(&kv);err!=nil{
			log.Printf("encode intermediante data for map task %d failed:%v",task.TaskID,err,)
			cleanup()
			return false
		}
	}
	for i,f:=range tempFiles{
		if err:=f.Close();err!=nil{
			log.Printf("close file failed%v",err)
			cleanup()
			return false
		}
		tempFiles[i]=nil
	}
	for i:=0;i<task.NReduce;i++{
		finalName:=fmt.Sprintf("mr-%d-%d",task.TaskID,i)
		if err:=os.Rename(tempNames[i],finalName);err!=nil{
			log.Printf("rename %q to %q failed:%v",tempNames[i],finalName,err)
			cleanup()
			return false
		}
		tempNames[i]=""
	}
	return true
}
func executeReduceTask(task*TaskReply,reducef func(string,[]string)string,)bool{
	var kva []KeyValue
	for mapID:=0;mapID<task.NMap;mapID++{
		filename:=fmt.Sprintf("mr-%d-%d",mapID,task.TaskID)
		f,err := os.Open(filename)
		if err!=nil{
			log.Printf("open interdiate file %q failed:%v",filename,err)
			return false
		}
		decoder:=json.NewDecoder(f)
		for {
			var kv KeyValue
			err:=decoder.Decode(&kv)
		if err==io.EOF{
			break
		}
		if err!=nil{
			log.Printf("decode intermediate file %q failed:%v",filename,err)
			_=f.Close()
			return false
		}
		kva=append(kva,kv)
	}
	if err:=f.Close();err!=nil{
		log.Printf("close intermediate file %q failed:%v",filename,err)
		return false
	}
}
	sort.Slice(kva,func(i,j int)bool{
		return kva[i].Key<kva[j].Key
	})
	tempFile,err:=os.CreateTemp(".","mr-out-tmp-*")
	if err!=nil{
		log.Printf("create reduce temp file failed:%v",err)
		return false
	}
	tempName:=tempFile.Name()
	keepTemp:=false
	defer func(){
		if !keepTemp{
			_=os.Remove(tempName)
		}
	}()
	for i:=0;i<len(kva);{
		j:=i+1
		for j<len(kva)&&kva[j].Key==kva[i].Key{
			j++
		}
		values:=make([]string,0,j-i)
		for k:=i;k<j;k++{
			values=append(values,kva[k].Value)
		}
		output:=reducef(kva[i].Key,values)
		if _,err:=fmt.Fprintf(tempFile,"%v %v\n",kva[i].Key,output,);err!=nil{
			log.Printf("write reduce output failed:%v",err)
			_=tempFile.Close()
			return false
		}
		i=j
	}
	if err:=tempFile.Close();err!=nil{
		log.Printf("close reduce output failed:%v",err)
		return false
	}
	finalName:=fmt.Sprintf("mr-out-%d",task.TaskID)
	if err:=os.Rename(tempName,finalName);err!=nil{
		log.Printf("rename reduce output to %q failed:%v",finalName,err)
		return false
	}
	keepTemp=true
	return true
}
func reportTaskFinished(taskType TaskType,taskID int){
	args:=TaskFinishedArgs{
		Type:taskType,
		TaskID:taskID,
	}
	reply:=TaskFinishedReply{}
	if !call("Coordinator.ReportTaskFinished",&args,&reply){
		log.Printf("report completion failed:type=%s taskID=%d",taskType,taskID,)
	}
}
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
