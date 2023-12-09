package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
type logTopic string

const (
	dNetwork   = "NETW"
	dElection  = "ELEC"
	dAppend    = "APPD"
	dTimer     = "TIMR"
	dReplicate = "REPL"
	dCommit    = "CMIT"
	dApply     = "APPL"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	//debugVerbosity = 1
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func RawPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func ServerPrintf(topic logTopic, rf *Raft, format string, a ...interface{}) {
	args := []interface{}{
		rf.me, rf.currentTerm, rf.currentState.ToString(),
	}
	args = append(args, a...)
	RawPrintf(topic, "S%v(T%v, %v) "+format, args...)
}

func ServerRacePrintf(topic logTopic, rf *Raft, format string, a ...interface{}) {
	args := []interface{}{
		rf.me,
	}
	args = append(args, a...)
	RawPrintf(topic, "S%v(...) "+format, args...)
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
