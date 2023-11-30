package mr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode"
)

func TestFileCreate(t *testing.T) {
	worker := MapExecutionContext{
		WorkerId:          "1",
		MapFunction:       nil,
		NReduce:           10,
		WorkingTaskId:     "1",
		InputFiles:        []string{"./test_input"},
		OutputFilesStatus: make(map[string]outputFileStatus),
	}

	file1, _ := worker.getOutputFile(&KeyValue{
		Key:   "a",
		Value: "a",
	})
	file2, _ := worker.getOutputFile(&KeyValue{
		Key:   "b",
		Value: "b",
	})
	file3, _ := worker.getOutputFile(&KeyValue{
		Key:   "a",
		Value: "a",
	})

	println(file1.Name())
	println(file2.Name())
	println(file3.Name())
	println(file1 == file2)
	println(file1 == file3)
}

func TestEncoder(t *testing.T) {
	mapWorker := MapExecutionContext{
		WorkerId:          "1",
		MapFunction:       nil,
		NReduce:           1,
		WorkingTaskId:     "1",
		InputFiles:        []string{"./test_input"},
		OutputFilesStatus: make(map[string]outputFileStatus),
	}

	mapWorker.writeKV(&KeyValue{
		Key:   "a",
		Value: "a",
	})
	mapWorker.writeKV(&KeyValue{
		Key:   "b",
		Value: "b",
	})
	mapWorker.writeKV(&KeyValue{
		Key:   "c",
		Value: "c",
	})
	mapWorker.writeKV(&KeyValue{
		Key:   "a",
		Value: "a",
	})
	mapWorker.writeKV(&KeyValue{
		Key:   "a",
		Value: "a",
	})
	mapWorker.writeKV(&KeyValue{
		Key:   "a",
		Value: "a",
	})

	filenames := mapWorker.atomicRename()

	reduceWorker := ReduceExecutionContext{
		WorkerId:       "1",
		ReduceFunction: func(key string, values []string) string { return strconv.Itoa(len(values)) },
		WorkingTaskId:  "0",
	}
	reduceWorker.SetInputFiles(filenames[reduceWorker.WorkingTaskId])

	reduceWorker.Execute()
}

func TestWordCountSingleMapTask(t *testing.T) {
	Map := func(filename string, contents string) []KeyValue {
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		words := strings.FieldsFunc(contents, ff)
		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}
	Reduce := func(key string, values []string) string {
		return strconv.Itoa(len(values))
	}

	nReduce := 1
	mapWorker := MapExecutionContext{
		WorkerId:      "1",
		MapFunction:   Map,
		NReduce:       nReduce,
		WorkingTaskId: "1",
		InputFiles: []string{
			"../main/pg-being_ernest.txt",
			"../main/pg-dorian_gray.txt",
			"../main/pg-frankenstein.txt",
			"../main/pg-grimm.txt",
			"../main/pg-huckleberry_finn.txt",
			"../main/pg-metamorphosis.txt",
			"../main/pg-sherlock_holmes.txt",
			"../main/pg-tom_sawyer.txt",
		},
		OutputFilesStatus: make(map[string]outputFileStatus),
	}
	var reduceWorkers []ReduceExecutionContext
	for i := 0; i < nReduce; i++ {
		reduceWorker := ReduceExecutionContext{
			WorkerId:       strconv.Itoa(i),
			ReduceFunction: Reduce,
			WorkingTaskId:  strconv.Itoa(i),
		}
		reduceWorkers = append(reduceWorkers, reduceWorker)
	}

	mapWorker.Execute()
	filenames := mapWorker.atomicRename()

	wg := sync.WaitGroup{}
	for _, worker := range reduceWorkers {
		worker.SetInputFiles(filenames[worker.WorkerId])
		wg.Add(1)
		go func(worker1 ReduceExecutionContext) {
			worker1.Execute()
			wg.Done()
		}(worker)
	}
	wg.Wait()
}

func TestWordCountMultipleMapTask(t *testing.T) {
	Map := func(filename string, contents string) []KeyValue {
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		words := strings.FieldsFunc(contents, ff)
		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}
	Reduce := func(key string, values []string) string {
		return strconv.Itoa(len(values))
	}

	inputFilenamesFlat := []string{
		"../main/pg-being_ernest.txt",
		"../main/pg-dorian_gray.txt",
		"../main/pg-frankenstein.txt",
		"../main/pg-grimm.txt",
		"../main/pg-huckleberry_finn.txt",
		"../main/pg-metamorphosis.txt",
		"../main/pg-sherlock_holmes.txt",
		"../main/pg-tom_sawyer.txt",
	}
	nMap := 5
	nReduce := 5

	// partition input files
	nInputFiles := len(inputFilenamesFlat)
	nFilesPerWorker := int(math.Ceil(float64(nInputFiles) / float64(nMap)))
	var inputFiles [][]string
	for i := 0; i < nMap; i++ {
		start := i * nFilesPerWorker
		end := -1
		if start < nInputFiles {
			end = start + nFilesPerWorker
			if end > nInputFiles {
				end = nInputFiles
			}
			inputFiles = append(inputFiles, inputFilenamesFlat[start:end])
		} else {
			inputFiles = append(inputFiles, make([]string, 0))
			start = -1
		}

		fmt.Printf("worker %v, start %v end %v\n", i, start, end)
	}

	var mapWorkers []MapExecutionContext
	for i := 0; i < nMap; i++ {
		mapWorker := MapExecutionContext{
			WorkerId:          strconv.Itoa(i),
			MapFunction:       Map,
			NReduce:           nReduce,
			WorkingTaskId:     strconv.Itoa(i),
			InputFiles:        inputFiles[i],
			OutputFilesStatus: make(map[string]outputFileStatus),
		}
		mapWorkers = append(mapWorkers, mapWorker)
	}

	var reduceWorkers []ReduceExecutionContext
	for i := 0; i < nReduce; i++ {
		reduceWorker := ReduceExecutionContext{
			WorkerId:       strconv.Itoa(i),
			ReduceFunction: Reduce,
			WorkingTaskId:  strconv.Itoa(i),
		}
		reduceWorkers = append(reduceWorkers, reduceWorker)
	}

	filenamesMutex := sync.Mutex{}
	filenames := make(map[string][]string)

	mapWg := sync.WaitGroup{}
	for _, worker := range mapWorkers {
		mapWg.Add(1)
		go func(worker1 MapExecutionContext) {
			worker1.Execute()
			result := worker1.atomicRename()
			filenamesMutex.Lock()
			for taskId, files := range result {
				tNames := filenames[taskId]
				for _, file := range files {
					tNames = append(tNames, file)
				}
				filenames[taskId] = tNames
			}
			filenamesMutex.Unlock()
			mapWg.Done()
		}(worker)
	}
	mapWg.Wait()

	reduceWg := sync.WaitGroup{}
	for _, worker := range reduceWorkers {
		worker.SetInputFiles(filenames[worker.WorkerId])
		reduceWg.Add(1)
		go func(worker1 ReduceExecutionContext) {
			worker1.Execute()
			reduceWg.Done()
		}(worker)
	}
	reduceWg.Wait()
}
