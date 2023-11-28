package mr

import "github.com/google/uuid"

type WorkerExecutionContext interface {
	Execute()
}

type WorkerExecutor struct {
	WorkerId       string
	MapFunction    func(string, string) []KeyValue
	ReduceFunction func(string, []string) string
}

type WorkerTask struct {
	Id         string
	Type       TaskType
	InputFiles []string
	NReduce    int
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

func (e *WorkerExecutor) fetchTask() WorkerTask {
	// RPC call to the coordinator
	return WorkerTask{}
}

func (e *WorkerExecutor) assignTask(task WorkerTask) WorkerExecutionContext {
	switch task.Type {
	case Map:
		return MakeMapExecutionContext(e, &task)
	case Reduce:
		return MakeReduceExecutionContext(e, &task)
	}
	return nil
}

func (e *WorkerExecutor) commitTask(task WorkerTask) {
	// RPC call to the coordinator

}

func (e *WorkerExecutor) Start() {
	if !e.registerWorker() {
		return
	}
	// Execution loop
	for {
		task := e.fetchTask()
		if task.Type == Exit {
			break
		}
		execCtx := e.assignTask(task)
		execCtx.Execute()
		e.commitTask(task)
	}
}
