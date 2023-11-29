package mr

import "github.com/google/uuid"

type ReduceTaskOutputFiles []string

type WorkerExecutionContext interface {
	Execute()
	GetMapTaskOutputFiles() (MapTaskOutputFiles, error)
	GetReduceTaskOutputFiles() (ReduceTaskOutputFiles, error)
}

type WorkerExecutor struct {
	WorkerId       string
	MapFunction    func(string, string) []KeyValue
	ReduceFunction func(string, []string) string
	NReduce        int
}

type WorkerTask struct {
	TaskId       string
	TaskType     TaskType
	InputFiles   []string
	NReduce      int
	MapResult    MapTaskOutputFiles
	ReduceResult ReduceTaskOutputFiles
}

func MakeWorkerExecutor(mapFunction func(string, string) []KeyValue,
	reduceFunction func(string, []string) string) *WorkerExecutor {
	workerUUID, err := uuid.NewUUID()
	if err != nil {
		return nil
	}
	return &WorkerExecutor{
		WorkerId:       workerUUID.String(),
		MapFunction:    mapFunction,
		ReduceFunction: reduceFunction,
	}
}

func (e *WorkerExecutor) registerWorker() bool {
	// RPC call to the coordinator
	var response RegisterWorkerResponse
	if ok := call("Coordinator.RegisterWorker",
		&RegisterWorkerArgs{e.WorkerId},
		&response); ok {
		return response.Successful
	}
	return false
}

func (e *WorkerExecutor) fetchTask() *WorkerTask {
	fetchReply := FetchTaskReply{}
	if call("Coordinator.FetchTask", &FetchTaskArgs{WorkerId: e.WorkerId}, &fetchReply) {
		return &WorkerTask{
			TaskId:     fetchReply.TaskId,
			TaskType:   fetchReply.TaskType,
			InputFiles: fetchReply.InputFiles,
			NReduce:    e.NReduce,
		}
	}
	return nil
}

func (e *WorkerExecutor) assignTask(task *WorkerTask) WorkerExecutionContext {
	switch task.TaskType {
	case MapTask:
		return MakeMapExecutionContext(e, task)
	case ReduceTask:
		return MakeReduceExecutionContext(e, task)
	}
	return nil
}

func (e *WorkerExecutor) commitTask(task *WorkerTask) {
	commitReply := CommitTaskReply{}
	switch task.TaskType {
	case MapTask:
		call("Coordinator.CommitTask", &CommitTaskArgs{
			WorkerId:           e.WorkerId,
			TaskId:             task.TaskId,
			TaskType:           task.TaskType,
			MapTaskOutputFiles: task.MapResult,
		}, commitReply)
	case ReduceTask:
		call("Coordinator.CommitTask", &CommitTaskArgs{
			WorkerId:              e.WorkerId,
			TaskId:                task.TaskId,
			TaskType:              task.TaskType,
			ReduceTaskOutputFiles: task.ReduceResult,
		}, &commitReply)
	}
}

func (e *WorkerExecutor) Start() {
	if !e.registerWorker() {
		return
	}

	nReduceReply := FetchNReduceReply{}
	if !call("Coordinator.GetNReduce", &FetchNReduceArgs{}, &nReduceReply) {
		return
	}
	e.NReduce = nReduceReply.NReduce

	// Execution loop
	for {
		task := e.fetchTask()
		if task.TaskType == ExitTask {
			break
		}
		execCtx := e.assignTask(task)
		execCtx.Execute()
		switch task.TaskType {
		case MapTask:
			if result, err := execCtx.GetMapTaskOutputFiles(); err == nil {
				task.MapResult = result
			}
		case ReduceTask:
			if result, err := execCtx.GetReduceTaskOutputFiles(); err == nil {
				task.ReduceResult = result
			}
		}
		e.commitTask(task)
	}
}
