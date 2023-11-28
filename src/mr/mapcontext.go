package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type outputFileStatus struct {
	//Key            string
	ReduceTaskId   string
	OutputFileName string // should do an atomic name change to take effect
	OutputFile     *os.File
	JsonEncoder    *json.Encoder
}

type MapExecutionContext struct {
	// worker specific
	WorkerId    string
	MapFunction func(string, string) []KeyValue
	NReduce     int // in this implementation, reduce tasks are fixed once the coordinator is set up
	// task specific
	WorkingTaskId     string
	InputFiles        []string
	OutputFilesStatus map[string]outputFileStatus // map: ReduceTaskId -> outputFileStatus
}

func MakeMapExecutionContext(executor *WorkerExecutor, task *WorkerTask) *MapExecutionContext {
	return &MapExecutionContext{
		WorkerId:          executor.WorkerId,
		MapFunction:       executor.MapFunction,
		NReduce:           task.NReduce,
		WorkingTaskId:     task.Id,
		InputFiles:        task.InputFiles,
		OutputFilesStatus: make(map[string]outputFileStatus),
	}
}

func (ctx *MapExecutionContext) Execute() {
	fmt.Printf("execute on worker %v map task %v\n", ctx.WorkerId, ctx.WorkingTaskId)
	for _, filename := range ctx.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		resultKVs := ctx.MapFunction(filename, string(content))
		for _, kv := range resultKVs {
			ctx.writeKV(&kv)
		}
	}
}

func (ctx *MapExecutionContext) atomicRename() map[string][]string {
	m := make(map[string][]string)
	for _, status := range ctx.OutputFilesStatus {
		err := os.Rename(status.OutputFile.Name(), status.OutputFileName)
		if err != nil {
			log.Fatalf("cannot rename file %v to %v", status.OutputFile.Name(), status.OutputFileName)
			return nil
		}
		if oldFileNames, ok := m[status.ReduceTaskId]; !ok {
			m[status.ReduceTaskId] = []string{status.OutputFileName}
		} else {
			m[status.ReduceTaskId] = append(oldFileNames, status.OutputFileName)
		}
	}
	return m
}

func (ctx *MapExecutionContext) getOutputFile(kv *KeyValue) (*os.File, *json.Encoder) {
	reduceTaskId := strconv.Itoa(ihash(kv.Key) % ctx.NReduce)
	if status, ok := ctx.OutputFilesStatus[reduceTaskId]; ok {
		return status.OutputFile, status.JsonEncoder
	}
	filename := IntermediateFilename(ctx.WorkingTaskId, reduceTaskId)
	outputFile, err := os.CreateTemp(".", "mr-temp-")
	if err != nil {
		log.Fatalf("unable to create file %v", filename)
		return nil, nil
	}
	encoder := json.NewEncoder(outputFile)
	status := outputFileStatus{
		ReduceTaskId:   reduceTaskId,
		OutputFileName: filename,
		OutputFile:     outputFile,
		JsonEncoder:    encoder,
	}
	ctx.OutputFilesStatus[reduceTaskId] = status
	return outputFile, encoder
}

func (ctx *MapExecutionContext) writeKV(kv *KeyValue) {
	file, enc := ctx.getOutputFile(kv)
	if file == nil {
		return
	}
	err := enc.Encode(&kv)
	if err != nil {
		return
	}
}

func (ctx *MapExecutionContext) closeFiles() {
	for _, status := range ctx.OutputFilesStatus {
		status.OutputFile.Close()
	}
	ctx.OutputFilesStatus = nil
}

func (ctx *MapExecutionContext) cleanUps() {
	ctx.closeFiles()
}
