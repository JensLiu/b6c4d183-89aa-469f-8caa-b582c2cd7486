package mr

import (
	crand "crypto/rand"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
)

func StartTest(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	MakeCoordinator(inputFiles, nReduce)

	workerWg := sync.WaitGroup{}
	executors := make(map[string]*WorkerExecutor)
	for i := 0; i < nWorker; i++ {
		executor := MakeWorkerExecutor(mapFunction, reduceFunction)
		executors[executor.WorkerId] = executor
		workerWg.Add(1)
		go func(executor *WorkerExecutor, wg *sync.WaitGroup) {
			executor.Start()
			wg.Done()
		}(executor, &workerWg)
	}
	workerWg.Wait()

	time.Sleep(3 * time.Second)

}

var inputFiles = []string{
	"../main/pg-being_ernest.txt",
	"../main/pg-dorian_gray.txt",
	"../main/pg-frankenstein.txt",
	"../main/pg-grimm.txt",
	"../main/pg-huckleberry_finn.txt",
	"../main/pg-metamorphosis.txt",
	"../main/pg-sherlock_holmes.txt",
	"../main/pg-tom_sawyer.txt",
}
var nWorker = 100
var nReduce = 50

func TestIntegrationWordCount(t *testing.T) {
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
	StartTest(Map, Reduce)
}

func TestIntegrationIndexer(t *testing.T) {
	Map := func(document string, value string) (res []KeyValue) {
		m := make(map[string]bool)
		words := strings.FieldsFunc(value, func(x rune) bool { return !unicode.IsLetter(x) })
		for _, w := range words {
			m[w] = true
		}
		for w := range m {
			kv := KeyValue{w, document}
			res = append(res, kv)
		}
		return
	}

	Reduce := func(key string, values []string) string {
		sort.Strings(values)
		return fmt.Sprintf("%d %s", len(values), strings.Join(values, ","))
	}
	StartTest(Map, Reduce)
}

func TestIntegrationCrash(t *testing.T) {
	maybeCrash := func() {
		max := big.NewInt(1000)
		rr, _ := crand.Int(crand.Reader, max)
		if rr.Int64() < 330 {
			// crash!
			//os.Exit(1)
		} else if rr.Int64() < 660 {
			// delay for a while.
			maxms := big.NewInt(10 * 1000)
			ms, _ := crand.Int(crand.Reader, maxms)
			time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
		}
	}

	Map := func(filename string, contents string) []KeyValue {
		maybeCrash()

		kva := []KeyValue{}
		kva = append(kva, KeyValue{"a", filename})
		kva = append(kva, KeyValue{"b", strconv.Itoa(len(filename))})
		kva = append(kva, KeyValue{"c", strconv.Itoa(len(contents))})
		kva = append(kva, KeyValue{"d", "xyzzy"})
		return kva
	}

	Reduce := func(key string, values []string) string {
		maybeCrash()

		// sort values to ensure deterministic output.
		vv := make([]string, len(values))
		copy(vv, values)
		sort.Strings(vv)

		val := strings.Join(vv, " ")
		return val
	}

	StartTest(Map, Reduce)
}

func TestIntegrationJobCount(t *testing.T) {
	var count int
	Map := func(filename string, contents string) []KeyValue {
		me := os.Getpid()
		f := fmt.Sprintf("mr-worker-jobcount-%d-%d", me, count)
		count++
		err := ioutil.WriteFile(f, []byte("x"), 0666)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
		return []KeyValue{KeyValue{"a", "x"}}
	}

	Reduce := func(key string, values []string) string {
		files, err := ioutil.ReadDir(".")
		if err != nil {
			panic(err)
		}
		invocations := 0
		for _, f := range files {
			if strings.HasPrefix(f.Name(), "mr-worker-jobcount") {
				invocations++
			}
		}
		return strconv.Itoa(invocations)
	}
	StartTest(Map, Reduce)
}
