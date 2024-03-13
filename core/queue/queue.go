package queue

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
)

// Task is a wrapper for encoding a task
type Task struct {
	Type       string          `json:"type"`
	OwnerID    int             `json:"-"`
	Task       json.RawMessage `json:"task"`
	QueuedOn   time.Time       `json:"queued_on"`
	ErrorCount int             `json:"error_count,omitempty"`
}

// Priority is the priority for the task
type Priority int

const (
	// HighPriority is the highest priority for tasks
	HighPriority = Priority(-10000000)

	// DefaultPriority is the default priority for tasks
	DefaultPriority = Priority(0)

	// LowPriority is the lowest priority for tasks
	LowPriority = Priority(+10000000)
)

const (
	queuePattern  = "%s:%d"
	activePattern = "%s:active"

	// BatchQueue is our queue for batch tasks, most things that operate on more than one cotact at a time
	BatchQueue = "batch"

	// HandlerQueue is our queue for message handling or other tasks related to just one contact
	HandlerQueue = "handler"
)

// Push adds the passed in task to our queue for execution
func Push(rc redis.Conn, queue string, taskType string, ownerID int, task any, priority Priority) error {
	score := score(priority)

	taskBody, err := json.Marshal(task)
	if err != nil {
		return err
	}

	wrapper := &Task{Type: taskType, OwnerID: ownerID, Task: taskBody, QueuedOn: dates.Now()}
	marshaled := jsonx.MustMarshal(wrapper)

	rc.Send("ZADD", fmt.Sprintf(queuePattern, queue, ownerID), score, marshaled)
	rc.Send("ZINCRBY", fmt.Sprintf(activePattern, queue), 0, ownerID) // ensure exists in active set
	_, err = rc.Do("")
	return err
}

func score(priority Priority) string {
	s := float64(dates.Now().UnixMicro())/float64(1000000) + float64(priority)
	return strconv.FormatFloat(s, 'f', 6, 64)
}

//go:embed lua/pop.lua
var popLua string
var popScript = redis.NewScript(1, popLua)

// Pop pops the next task off our queue
func Pop(rc redis.Conn, queue string) (*Task, error) {
	task := &Task{}
	for {
		values, err := redis.Strings(popScript.Do(rc, fmt.Sprintf(activePattern, queue), queue))
		if err != nil {
			return nil, err
		}

		if values[0] == "empty" {
			return nil, nil
		}

		if values[0] == "retry" {
			continue
		}

		err = json.Unmarshal([]byte(values[1]), task)
		if err != nil {
			return nil, err
		}

		ownerID, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, err
		}

		task.OwnerID = ownerID

		return task, err
	}
}

//go:embed lua/done.lua
var doneLua string
var doneScript = redis.NewScript(1, doneLua)

// Done marks the passed in task as complete. Callers must call this in order
// to maintain fair workers across orgs
func Done(rc redis.Conn, queue string, ownerID int) error {
	_, err := doneScript.Do(rc, fmt.Sprintf(activePattern, queue), strconv.FormatInt(int64(ownerID), 10))
	return err
}

//go:embed lua/size.lua
var sizeLua string
var sizeScript = redis.NewScript(1, sizeLua)

// Size returns the total number of tasks for the passed in queue across all owners
func Size(rc redis.Conn, queue string) (int, error) {
	return redis.Int(sizeScript.Do(rc, fmt.Sprintf(activePattern, queue), queue))
}
