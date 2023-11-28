package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkerStatus int

const (
	WorkerIdle WorkerStatus = iota
	WorkerInProgress
	WorkerCompleted
)

type TaskStatus int

const (
	TaskIdle TaskStatus = iota
	TaskInProgress
	TaskCompleted
)

type WorkerInfo struct {
	WorkerId     string
	WorkerStatus WorkerStatus
}

type mapTaskInfo struct {
	MapTaskId         string
	ExecutionStatus   TaskStatus
	ExecutingWorkerId string
	InputFiles        []string
}

type reduceTaskInfo struct {
	ReduceTaskId          string
	ExecutionStatus       TaskStatus
	ExecutingWorkerId     string     // current executing worker id
	InputFiles            [][]string // incoming finished map task may add new files to this task
	InputProcessingCursor int        // points to the processing iteration in InputFiles
}

type incrementalUpdateInfo struct {
	TaskId    string
	TaskType  TaskType
	Filenames []string
}

type Coordinator struct {
	// counters
	NextMapTaskId    int
	NextReduceTaskId int
	TaskIdMutex      sync.Mutex
	// contexts
	MapTasks      map[string]mapTaskInfo           // map: MapTaskId -> taskInfo
	MapMutex      sync.Mutex                       // locks MapTasks
	ReduceTasks   map[string]reduceTaskInfo        // map: ReduceTaskId -> taskInfo
	ReduceMutex   sync.Mutex                       // locks ReduceTasks
	ReduceUpdates map[string]incrementalUpdateInfo // map: ReduceTaskId -> incrementalInfo
	UpdateMutex   sync.Mutex                       // locks ReduceUpdates
	WorkerInfos   map[string]WorkerInfo            // map: WorkerId -> workerInfo
	WorkerMutex   sync.Mutex                       // locks WorkerInfos
}

func (ctx *Coordinator) scheduleOneMapTask() *mapTaskInfo {
	for _, info := range ctx.MapTasks {
		if info.ExecutionStatus == TaskIdle {
			return &info
		}
	}
	return nil
}

func (ctx *Coordinator) scheduleOneReduceTask() *reduceTaskInfo {
	for taskId, info := range ctx.ReduceTasks {
		ctx.ReduceMutex.Lock()
		if info.ExecutionStatus == TaskIdle {
			if info.InputProcessingCursor < len(info.InputFiles)-1 {
				info.InputProcessingCursor++
				ctx.ReduceMutex.Unlock()
				return &info
			}
			ctx.UpdateMutex.Lock()
			if update, ok := ctx.ReduceUpdates[taskId]; ok {
				info.InputFiles = append(info.InputFiles, update.Filenames)
				delete(ctx.ReduceUpdates, taskId)
				ctx.UpdateMutex.Unlock()
				info.InputProcessingCursor++
				ctx.ReduceMutex.Unlock()
				return &info
			}
			ctx.UpdateMutex.Unlock()
		}
		ctx.ReduceMutex.Unlock()
	}
	return nil
}

// PC handlers for the worker to call.

func (ctx *Coordinator) RegisterWorker(args RegisterWorkerArgs, response RegisterWorkerResponse) error {
	ctx.WorkerMutex.Lock()
	defer ctx.WorkerMutex.Unlock()

	if _, ok := ctx.WorkerInfos[args.WorkerId]; ok {
		response.Successful = true
		return nil
	}
	// add to worker
	workerInfo := WorkerInfo{
		WorkerId:     args.WorkerId,
		WorkerStatus: WorkerIdle,
	}
	ctx.WorkerInfos[args.WorkerId] = workerInfo
	response.Successful = true
	return nil
}

func (ctx *Coordinator) isWorkerIdle(workerId string) bool {
	ctx.WorkerMutex.Lock()
	defer ctx.WorkerMutex.Unlock()
	return ctx.WorkerInfos[workerId].WorkerStatus == WorkerIdle
}

func (ctx *Coordinator) FetchTask(args FetchTaskArgs, response FetchTaskResponse) error {
	if !ctx.isWorkerIdle(args.WorkerId) {
		log.Fatalf("worker %v is not idle", args.WorkerId)
		return nil
	}
	if task := ctx.scheduleOneMapTask(); task != nil {

	}
	if task := ctx.scheduleOneReduceTask(); task != nil {

	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (ctx *Coordinator) server() {
	rpc.Register(ctx)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (ctx *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.

	c.server()
	return &c
}
