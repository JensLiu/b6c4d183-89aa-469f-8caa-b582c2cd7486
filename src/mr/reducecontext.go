package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

type inputFileStatus struct {
	InputFile   *os.File
	JsonDecoder *json.Decoder
}

type ReduceExecutionContext struct {
	// worker specific
	WorkerId       string
	ReduceFunction func(string, []string) string
	// task specific
	WorkingTaskId   string
	InputFileStatus []inputFileStatus
	Result          ReduceTaskOutputFiles
}

func MakeReduceExecutionContext(executor *WorkerExecutor, task *WorkerTask) *ReduceExecutionContext {
	ctx := &ReduceExecutionContext{
		WorkerId:       executor.WorkerId,
		ReduceFunction: executor.ReduceFunction,
		WorkingTaskId:  task.TaskId,
	}
	ctx.SetInputFiles(task.InputFiles)
	return ctx
}

func (ctx *ReduceExecutionContext) SetInputFiles(filenames []string) {
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}
		ctx.InputFileStatus = append(ctx.InputFileStatus,
			inputFileStatus{
				InputFile:   file,
				JsonDecoder: json.NewDecoder(file),
			})
	}
}

func (ctx *ReduceExecutionContext) inputInMemorySort() []KeyValue {
	var kvs []KeyValue
	for _, status := range ctx.InputFileStatus {
		for {
			dec := status.JsonDecoder
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	return kvs
}

func (ctx *ReduceExecutionContext) Execute() {
	fmt.Printf("execute on worker %v reduce task %v\n", ctx.WorkerId, ctx.WorkingTaskId)
	outputFilename := "mr-out-" + ctx.WorkingTaskId
	outputFile, err := os.CreateTemp(".", "mr-out-temp-")
	if err != nil {
		log.Fatalf("cannot create %v", outputFilename)
		return
	}
	defer outputFile.Close()
	intermediate := ctx.inputInMemorySort()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := ctx.ReduceFunction(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(outputFile.Name(), outputFilename)
	ctx.Result = []string{outputFilename}
	fmt.Printf("worker %v reduce task %v finished\n", ctx.WorkerId, ctx.WorkingTaskId)
}

func IntermediateFilename(mapTaskId string, reduceTaskId string) string {
	return fmt.Sprintf("mr-%v-%v", mapTaskId, reduceTaskId)
}

func (ctx *ReduceExecutionContext) GetMapTaskOutputFiles() (MapTaskOutputFiles, error) {
	return nil, fmt.Errorf("not implemented")
}

func (ctx *ReduceExecutionContext) GetReduceTaskOutputFiles() (ReduceTaskOutputFiles, error) {
	return ctx.Result, nil
}
