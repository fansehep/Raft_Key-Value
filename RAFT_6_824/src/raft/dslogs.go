package raft
import(
	"os"
	"log"
	"strconv"
	"time"
	"fmt"
	"sync"
)
// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")	
	level := 0
	if v != ""{
		var err error
		level, err = strconv.Atoi(v)
		if err != nil{
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string
const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var mutex sync.Mutex

func LOGinit() {
	mutex.Lock()
	if debugVerbosity == 0{
		debugVerbosity = getVerbosity()
		debugStart = time.Now()
	}
	mutex.Unlock()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DEBUG(topic logTopic, format string, a ...interface{}) {
	// if debugVerbosity > 1 {
		mutex.Lock()
		time := time.Since(debugStart).Microseconds()
		mutex.Unlock()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	// }
}