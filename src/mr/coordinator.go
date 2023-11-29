package mr

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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
	WorkerId          string
	WorkerStatus      WorkerStatus
	ExecutingTaskId   string
	ExecutingTaskType TaskType
	Badness           int // worker timeout count
}

type simpleTaskInfo struct {
	// task specific
	TaskId     string
	TaskType   TaskType
	InputFiles []string
	// execution specific
	ExecutionStatus              TaskStatus
	MostRecentlyAssignedWorkerId string
	// if a worker times out, the coordinator would recycle the task
	// the timeout function need to know if the task has been recycled
	// by checking if this field is the worker's id
}

type incrementalUpdateInfo struct {
	TaskId    string
	TaskType  TaskType
	Filenames []string
}

type Coordinator struct {
	UnfinishedMapTaskCount    atomic.Int32
	UnfinishedReduceTaskCount atomic.Int32

	// WorkerTimeoutCount
	// a worker may time out, there's a corner case where the map workers time out frequently
	// while reduce workers are pending for map to finish, thus they will never start
	// we address this by forcing reduce workers to check for this timeout count after being woken up
	// if the timeout count increased, it means that some map task timed out and the reduce task worker might
	// be scheduled to be the next map worker
	WorkerTimeoutCount         atomic.Int32
	CondAllMapTasksFinished    *sync.Cond
	CondAllReduceTasksFinished *sync.Cond

	MapTasks              map[string]*simpleTaskInfo // map: MapTaskId -> taskInfo
	MapMutex              *sync.Mutex                // locks MapTasks
	ReduceTasks           map[string]*simpleTaskInfo // map: ReduceTaskId -> taskInfo
	ReduceMutex           *sync.Mutex                // locks ReduceTasks
	WorkerInfos           map[string]*WorkerInfo     // map: WorkerId -> workerInfo
	WorkerMutex           *sync.Mutex                // locks WorkerInfos
	WorkerTimeoutDuration time.Duration
}

func (ctx *Coordinator) InitMapTasks(inputFiles []string) {
	// we by default schedule one map task per file
	for mapTaskId, mapTaskFile := range inputFiles {
		ctx.MapTasks[strconv.Itoa(mapTaskId)] = &simpleTaskInfo{
			TaskId:          strconv.Itoa(mapTaskId),
			TaskType:        MapTask,
			ExecutionStatus: TaskIdle,
			InputFiles:      []string{mapTaskFile},
		}
	}
	ctx.UnfinishedMapTaskCount.Store(int32(len(ctx.MapTasks)))
}

func (ctx *Coordinator) InitReduceTasks(nReduce int) {
	for reduceTaskId := 0; reduceTaskId < nReduce; reduceTaskId++ {
		ctx.ReduceTasks[strconv.Itoa(reduceTaskId)] = &simpleTaskInfo{
			TaskId:          strconv.Itoa(reduceTaskId),
			TaskType:        ReduceTask,
			ExecutionStatus: TaskIdle,
			InputFiles:      make([]string, 0),
		}
	}
	ctx.UnfinishedReduceTaskCount.Store(int32(len(ctx.ReduceTasks)))
}

func (ctx *Coordinator) InitLocks() {
	ctx.MapMutex = &sync.Mutex{}
	ctx.ReduceMutex = &sync.Mutex{}
	ctx.WorkerMutex = &sync.Mutex{}
	ctx.CondAllMapTasksFinished = sync.NewCond(ctx.MapMutex)
	ctx.CondAllReduceTasksFinished = sync.NewCond(ctx.ReduceMutex)
}

func (ctx *Coordinator) InitMetadata() {
	ctx.MapTasks = make(map[string]*simpleTaskInfo)
	ctx.ReduceTasks = make(map[string]*simpleTaskInfo)
	ctx.WorkerInfos = make(map[string]*WorkerInfo)
}

func (ctx *Coordinator) tryScheduleOneMapTask(workerId string) *simpleTaskInfo {
	for taskId, info := range ctx.MapTasks {
		if info.ExecutionStatus == TaskIdle {
			info.ExecutionStatus = TaskInProgress
			info.MostRecentlyAssignedWorkerId = workerId
			ctx.MapTasks[taskId] = info
			return info
		}
	}
	return nil
}

func (ctx *Coordinator) tryScheduleOneReduceTask(workerId string) *simpleTaskInfo {
	for taskId, info := range ctx.ReduceTasks {
		if info.ExecutionStatus == TaskIdle {
			info.ExecutionStatus = TaskInProgress
			info.MostRecentlyAssignedWorkerId = workerId
			ctx.ReduceTasks[taskId] = info
			return info
		}
	}
	return nil
}

// lock dependency: none?
func (ctx *Coordinator) schedule(workerId string) *simpleTaskInfo {
	// NOTE(jens): simplification
	// workers with WorkerId greater than all map tasks are considered to be assigned reduce tasks
	// and will block here until all map tasks are finished

	if !ctx.isWorkerIdle(workerId) {
		//log.Fatalf("worker %v not idle, cannot schedule", workerId)
		log.Printf("worker %v not idle, cannot schedule", workerId)
		return nil
	}

schedule:
	for {
		localWorkerTimeoutCount := ctx.WorkerTimeoutCount.Load()
		// MapMutex scope begin
		ctx.MapMutex.Lock()
		task := ctx.tryScheduleOneMapTask(workerId)
		if task != nil {
			ctx.MapMutex.Unlock()
			// WorkerMutex scope begin
			ctx.WorkerMutex.Lock()
			ctx.WorkerInfos[workerId] = &WorkerInfo{
				WorkerId:          workerId,
				WorkerStatus:      WorkerInProgress,
				ExecutingTaskId:   task.TaskId,
				ExecutingTaskType: task.TaskType,
			}
			ctx.WorkerMutex.Unlock()
			// WorkerMutex
			log.Printf("Worker %v, scheduled map-task %v\n", workerId, task.TaskId)
			return task
		}
		// all map tasks are scheduled, they are either in progress of finished
		for !ctx.UnfinishedMapTaskCount.CompareAndSwap(0, 0) { // not all map tasks are finished
			log.Printf("Worker %v, wait for all map-tasks to finish\n", workerId)
			ctx.CondAllMapTasksFinished.Wait()
			if localWorkerTimeoutCount < ctx.WorkerTimeoutCount.Load() {
				// maybe another thread died and the scheduler recycled the task
				ctx.MapMutex.Unlock()
				goto schedule
			}
		} // all map tasks are finished
		ctx.MapMutex.Unlock()
		// MapMutex scope end

		ctx.ReduceMutex.Lock()
		task = ctx.tryScheduleOneReduceTask(workerId)
		if task != nil {
			ctx.ReduceMutex.Unlock()
			log.Printf("Worker %v, scheduled reduce-task %v\n", workerId, task.TaskId)
			ctx.WorkerMutex.Lock()
			ctx.WorkerInfos[workerId] = &WorkerInfo{
				WorkerId:          workerId,
				WorkerStatus:      WorkerInProgress,
				ExecutingTaskId:   task.TaskId,
				ExecutingTaskType: task.TaskType,
			}
			ctx.WorkerMutex.Unlock()
			return task
		} // all reduce tasks are scheduled, they are either in progress of finished
		for {
			if ctx.UnfinishedReduceTaskCount.CompareAndSwap(0, 0) {
				// all reduce tasks completed
				ctx.ReduceMutex.Unlock()
				return nil
			} // not all reduce tasks are finished
			log.Printf("Worker %v, wait for all reduce tasks to finish\n", workerId)
			ctx.CondAllReduceTasksFinished.Wait()
			if localWorkerTimeoutCount < ctx.WorkerTimeoutCount.Load() {
				ctx.ReduceMutex.Unlock()
				goto schedule
			}
		}
	}
}

// lock dependency none
func (ctx *Coordinator) startWorkerTimer(workerId string, task simpleTaskInfo) {
	// do not pass by pointer to keep a local copy
	go func(workerId string, task simpleTaskInfo) {
		time.Sleep(ctx.WorkerTimeoutDuration)
		ctx.WorkerMutex.Lock()
		if workerInfo, ok := ctx.WorkerInfos[workerId]; !ok {
			ctx.WorkerMutex.Unlock()
			log.Fatalf("worker %v not found", workerId)
			return
		} else {
			if workerInfo.WorkerStatus == WorkerInProgress &&
				workerInfo.ExecutingTaskId == task.TaskId &&
				workerInfo.ExecutingTaskType == task.TaskType {
				// worker did not respond
				workerInfo.Badness++
				// recycle the worker
				ctx.WorkerInfos[workerId] = &WorkerInfo{
					WorkerId:          workerId,
					WorkerStatus:      WorkerIdle,
					ExecutingTaskId:   "",
					ExecutingTaskType: InvalidTask,
				}
				ctx.WorkerMutex.Unlock()

				ctx.WorkerTimeoutCount.Add(1)

				// recycle the task
				switch task.TaskType {
				case MapTask:
					log.Printf("timer went off: worker %v, recycle map-task %v", workerId, task.TaskId)
					ctx.MapMutex.Lock()
					if t, ok := ctx.MapTasks[task.TaskId]; ok && t.MostRecentlyAssignedWorkerId == workerId {
						// the worker timed out and the task has not been recycled
						t.ExecutionStatus = TaskIdle
						t.MostRecentlyAssignedWorkerId = ""
						ctx.MapTasks[task.TaskId] = t
					}
					ctx.MapMutex.Unlock()
				case ReduceTask:
					log.Printf("timer went off: worker %v, recycle reduce-task %v", workerId, task.TaskId)
					ctx.ReduceMutex.Lock()
					if t, ok := ctx.ReduceTasks[task.TaskId]; ok && t.MostRecentlyAssignedWorkerId == workerId {
						// the worker timed out and the task has not been recycled
						t.ExecutionStatus = TaskIdle
						t.MostRecentlyAssignedWorkerId = ""
						ctx.ReduceTasks[task.TaskId] = t
					}
					ctx.ReduceMutex.Unlock()
				}
				ctx.CondAllReduceTasksFinished.Broadcast()
				ctx.CondAllMapTasksFinished.Broadcast()
			} else {
				ctx.WorkerMutex.Unlock()
			}
		}
	}(workerId, task)
}

func (ctx *Coordinator) fetchOneTask(workerId string) *simpleTaskInfo {
	task := ctx.schedule(workerId)
	if task != nil {
		ctx.startWorkerTimer(workerId, *task)
	}
	return task
}

func (ctx *Coordinator) onCommitReleaseWorker(workerId string, localTaskType TaskType) *WorkerInfo {
	ctx.WorkerMutex.Lock()
	defer ctx.WorkerMutex.Unlock()

	if _, ok := ctx.WorkerInfos[workerId]; !ok {
		log.Fatalf("worker %v not found", workerId)
		return nil
	}

	if ctxTaskType := ctx.WorkerInfos[workerId].ExecutingTaskType; ctxTaskType == InvalidTask {
		log.Printf("worker %v is recycled, assume the worker dead and ignore its result", workerId)
		return nil
	} else if ctxTaskType != localTaskType {
		log.Fatalf("worker %v is not commiting a %v-task, %v", workerId, localTaskType, ctx.WorkerInfos[workerId])
		return nil
	}

	old := ctx.WorkerInfos[workerId]

	ctx.WorkerInfos[workerId] = &WorkerInfo{
		WorkerId:          workerId,
		WorkerStatus:      WorkerIdle,
		ExecutingTaskId:   "",
		ExecutingTaskType: InvalidTask,
	}

	return old
}

func (ctx *Coordinator) onCommitAcceptTask(workerInfo *WorkerInfo, taskType TaskType) *simpleTaskInfo {
	taskId := workerInfo.ExecutingTaskId
	log.Printf("worker %v committed %v-task %v\n", workerInfo.WorkerId, taskType, taskId)

	var task *simpleTaskInfo
	switch taskType {
	case MapTask:
		ctx.MapMutex.Lock()
		defer ctx.MapMutex.Unlock()
		if _, ok := ctx.MapTasks[taskId]; !ok {
			log.Fatalf("map-task %v not found", taskId)
			return nil
		}
		task = ctx.MapTasks[taskId]
		if task.ExecutionStatus == TaskCompleted {
			log.Printf("map-task %v is already done by another worker, current worker %v rejected",
				taskId, workerInfo.WorkerId)
			return nil
		}
		task.ExecutionStatus = TaskCompleted
		ctx.MapTasks[taskId] = task
		log.Printf("map-task %v state %v\n", taskId, ctx.MapTasks[taskId].ExecutionStatus)
	case ReduceTask:
		ctx.ReduceMutex.Lock()
		defer ctx.ReduceMutex.Unlock()
		if _, ok := ctx.ReduceTasks[taskId]; !ok {
			log.Fatalf("reduce-task %v not found", taskId)
			return nil
		}
		task = ctx.ReduceTasks[taskId]
		if task.ExecutionStatus == TaskCompleted {
			log.Printf("reduce-task %v is already done by another worker, current worker %v rejected",
				taskId, workerInfo.WorkerId)
			return nil
		}
		task.ExecutionStatus = TaskCompleted
		ctx.ReduceTasks[taskId] = task
		log.Printf("reduce-task %v state: %v\n", taskId, ctx.ReduceTasks[taskId].ExecutionStatus)
	}

	return task
}

func (ctx *Coordinator) CommitMapTask(workerId string, outputFiles MapTaskOutputFiles) {
	workerInfo := ctx.onCommitReleaseWorker(workerId, MapTask)
	if workerInfo == nil {
		return
	}
	task := ctx.onCommitAcceptTask(workerInfo, MapTask)
	if task == nil {
		return
	}

	// accept result
	// update reduce task's input files
	ctx.ReduceMutex.Lock()
	for reduceTaskId, files := range outputFiles {
		reduceTask, ok := ctx.ReduceTasks[reduceTaskId]
		if !ok {
			log.Fatalf("unable to find reduce-task %v", reduceTaskId)
		} else {
			inputFiles := reduceTask.InputFiles
			for _, file := range files {
				inputFiles = append(inputFiles, file)
			}
			reduceTask.InputFiles = inputFiles
			ctx.ReduceTasks[reduceTaskId] = reduceTask
		}
	}
	ctx.ReduceMutex.Unlock()

	// broadcast if all completed
	// Note(jens): to address the lost wakeup problem
	// 	lock first before modifying ctx.UnfinishedMapTaskCount
	ctx.MapMutex.Lock()
	if ctx.UnfinishedMapTaskCount.Add(-1) == 0 {
		ctx.CondAllMapTasksFinished.Broadcast()
	}
	ctx.MapMutex.Unlock()

}

func (ctx *Coordinator) CommitReduceTask(workerId string, outputFiles []string) {
	workerInfo := ctx.onCommitReleaseWorker(workerId, ReduceTask)
	if workerInfo == nil {
		return
	}
	task := ctx.onCommitAcceptTask(workerInfo, ReduceTask)
	if task == nil {
		return
	}

	// accept results
	//log.Printf("%v", outputFiles)

	// broadcast if all completed
	ctx.ReduceMutex.Lock()
	if ctx.UnfinishedReduceTaskCount.Add(-1) == 0 {
		ctx.CondAllReduceTasksFinished.Broadcast()
	}
	ctx.ReduceMutex.Unlock()
}

// PC handlers for the worker to call.

func (ctx *Coordinator) RegisterWorker(args RegisterWorkerArgs, response *RegisterWorkerResponse) error {
	ctx.WorkerMutex.Lock()
	defer ctx.WorkerMutex.Unlock()

	if _, ok := ctx.WorkerInfos[args.WorkerId]; ok {
		response.Successful = true
		return nil
	}
	// add to worker
	workerInfo := &WorkerInfo{
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

func (ctx *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	if !ctx.isWorkerIdle(args.WorkerId) {
		log.Fatalf("worker %v is not idle", args.WorkerId)
		return nil
	}
	task := ctx.fetchOneTask(args.WorkerId)
	if task == nil {
		reply.TaskType = ExitTask
		return nil
	}
	reply.TaskId = task.TaskId
	reply.TaskType = task.TaskType
	reply.InputFiles = task.InputFiles
	return nil
}

func (ctx *Coordinator) CommitTask(args *CommitTaskArgs, _ *CommitTaskReply) error {
	switch args.TaskType {
	case MapTask:
		ctx.CommitMapTask(args.WorkerId, args.MapTaskOutputFiles)
	case ReduceTask:
		ctx.CommitReduceTask(args.WorkerId, args.ReduceTaskOutputFiles)
	default:
		_ = fmt.Errorf("invalid task")
	}
	return nil
}

func (ctx *Coordinator) GetNReduce(_ *FetchNReduceArgs, reply *FetchNReduceReply) error {
	reply.NReduce = len(ctx.ReduceTasks)
	return nil
}

func PartitionMapTasks(inputFiles []string, nMap int) [][]string {
	nInputFiles := len(inputFiles)
	nFilesPerTask := int(math.Ceil(float64(nInputFiles) / float64(nMap)))
	var mapTaskFiles [][]string
	for i := 0; i < nMap; i++ {
		start := i * nFilesPerTask
		end := -1
		if start < nInputFiles {
			end = start + nFilesPerTask
			if end > nInputFiles {
				end = nInputFiles
			}
			mapTaskFiles = append(mapTaskFiles, inputFiles[start:end])
		} else {
			mapTaskFiles = append(mapTaskFiles, make([]string, 0))
			start = -1
		}
		log.Printf("worker %v, start %v end %v\n", i, start, end)
	}
	return mapTaskFiles
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
	return ctx.UnfinishedMapTaskCount.Load() == 0 && ctx.UnfinishedReduceTaskCount.Load() == 0
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.InitLocks()
	c.InitMetadata()
	c.InitMapTasks(files)
	c.InitReduceTasks(nReduce)
	c.WorkerTimeoutDuration = 10 * time.Second
	c.server()
	return &c
}
