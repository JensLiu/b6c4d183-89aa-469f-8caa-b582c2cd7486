package mr

import (
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestCoordinator(t *testing.T) {

	nReduce := 20
	nWorker := 1000
	timeoutMs := 500
	mapMaxMs := 10000
	reduceMaxMs := 10000
	inputFiles := []string{
		"../main/pg-being_ernest.txt",
		"../main/pg-dorian_gray.txt",
		"../main/pg-frankenstein.txt",
		"../main/pg-grimm.txt",
		"../main/pg-huckleberry_finn.txt",
		"../main/pg-metamorphosis.txt",
		"../main/pg-sherlock_holmes.txt",
		"../main/pg-tom_sawyer.txt",
	}

	c := Coordinator{}
	c.InitLocks()
	c.InitMetadata()
	c.InitMapTasks(inputFiles)
	c.InitReduceTasks(nReduce)
	c.WorkerTimeoutDuration = time.Duration(timeoutMs) * time.Millisecond
	go c.server()

	workerIds := make([]string, 0)
	for i := 0; i < nWorker; i++ {
		workerId, _ := uuid.NewUUID()
		workerIds = append(workerIds, workerId.String())
		reg_res := RegisterWorkerResponse{}
		c.RegisterWorker(RegisterWorkerArgs{WorkerId: workerIds[i]}, &reg_res)
	}

	// ask workers to fetch tasks
	// some will block
	type pseudoWorkerInfo struct {
		WorkerId string
		TaskId   string
		Idle     bool
	}
	mapMutex := sync.Mutex{}
	mapWorkers := make(map[string]pseudoWorkerInfo)
	reduceMutex := sync.Mutex{}
	reduceWorkers := make(map[string]pseudoWorkerInfo)
	mapPseudoCommit := func(workerId, mapTaskId string) {
		outputFiles := make(map[string][]string)
		for _, worker := range reduceWorkers {
			outputFiles[worker.TaskId] = []string{
				fmt.Sprintf("mr-%v-%v", mapTaskId, worker.TaskId),
			}
		}
		c.CommitMapTask(workerId, outputFiles)
	}

	workerWg := sync.WaitGroup{}
	func() {
		for i := 0; i < nWorker; i++ {
			workerWg.Add(1)
			go func(c *Coordinator, workerId string, workerWg *sync.WaitGroup,
				mapWorkers map[string]pseudoWorkerInfo, reduceWorkers map[string]pseudoWorkerInfo) {
				for {
					fetchReply := FetchTaskReply{}
					c.FetchTask(&FetchTaskArgs{WorkerId: workerId}, &fetchReply)
					if fetchReply.TaskType == MapTask {
						mapMutex.Lock()
						mapWorkers[workerId] = pseudoWorkerInfo{
							WorkerId: workerId,
							TaskId:   fetchReply.TaskId,
							Idle:     false,
						}
						mapMutex.Unlock()
						time.Sleep(time.Duration(rand.Intn(mapMaxMs)) * time.Microsecond)
						mapPseudoCommit(workerId, fetchReply.TaskId)
						mapMutex.Lock()
						mapWorkers[workerId] = pseudoWorkerInfo{
							WorkerId: workerId,
							TaskId:   "",
							Idle:     true,
						}
						mapMutex.Unlock()
					} else if fetchReply.TaskType == ReduceTask {
						reduceMutex.Lock()
						reduceWorkers[workerId] = pseudoWorkerInfo{
							WorkerId: workerId,
							TaskId:   fetchReply.TaskId,
							Idle:     false,
						}
						reduceMutex.Unlock()
						time.Sleep(time.Duration(rand.Intn(reduceMaxMs)) * time.Millisecond)
						c.CommitReduceTask(workerId, make([]string, 0))
						reduceMutex.Lock()
						reduceWorkers[workerId] = pseudoWorkerInfo{
							WorkerId: workerId,
							TaskId:   "",
							Idle:     true,
						}
						reduceMutex.Unlock()
					} else if fetchReply.TaskType == ExitTask {
						fmt.Printf("exit\n")
						workerWg.Done()
						return
					}
				}
			}(&c, workerIds[i], &workerWg, mapWorkers, reduceWorkers)
		}
	}()

	workerWg.Wait()

	//worker1 := MakeWorkerExecutor(Map, Reduce)
	//worker2 := MakeWorkerExecutor(Map, Reduce)
	//worker3 := MakeWorkerExecutor(Map, Reduce)

	//go worker1.Start()
	//go worker2.Start()
	//go worker3.Start()
}

func TestWorkerTimeout(t *testing.T) {
	inputFiles := []string{
		"../main/pg-being_ernest.txt",
		"../main/pg-dorian_gray.txt",
		"../main/pg-frankenstein.txt",
		"../main/pg-grimm.txt",
		"../main/pg-huckleberry_finn.txt",
		"../main/pg-metamorphosis.txt",
		"../main/pg-sherlock_holmes.txt",
		"../main/pg-tom_sawyer.txt",
	}
	nReduce := 10
	nWorker := 10

	c := Coordinator{}
	c.InitLocks()
	c.InitMetadata()
	c.InitMapTasks(inputFiles)
	c.InitReduceTasks(nReduce)
	c.WorkerTimeoutDuration = 500 * time.Millisecond
	go c.server()

	workerIds := make([]string, 0)
	for i := 0; i < nWorker; i++ {
		workerId, _ := uuid.NewUUID()
		workerIds = append(workerIds, workerId.String())
		reg_res := RegisterWorkerResponse{}
		c.RegisterWorker(RegisterWorkerArgs{WorkerId: workerIds[i]}, &reg_res)
	}

	for i := 0; i < nWorker; i++ {
		workerId := workerIds[i]
		reply := FetchTaskReply{}
		c.FetchTask(&FetchTaskArgs{WorkerId: workerId}, &reply)
		time.Sleep(2 * time.Second)
	}
	type pseudoWorkerInfo struct {
		WorkerId string
		TaskId   string
		Idle     bool
	}

	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

}
