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
	"github.com/pkg/errors"
)

// Task is a wrapper for encoding a task
type Task struct {
	Type       string          `json:"type"`
	OrgID      int             `json:"org_id"`
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

// Size returns the number of tasks for the passed in queue
func Size(rc redis.Conn, queue string) (int, error) {
	// get all the active queues
	queues, err := redis.Ints(rc.Do("zrange", fmt.Sprintf(activePattern, queue), 0, -1))
	if err != nil {
		return 0, errors.Wrapf(err, "error getting active queues for: %s", queue)
	}

	// add up each
	size := 0
	for _, q := range queues {
		count, err := redis.Int(rc.Do("zcard", fmt.Sprintf(queuePattern, queue, q)))
		if err != nil {
			return 0, errors.Wrapf(err, "error getting size of: %d", q)
		}
		size += count
	}

	return size, nil
}

// Push adds the passed in task to our queue for execution
func Push(rc redis.Conn, queue string, taskType string, orgID int, task any, priority Priority) error {
	score := score(priority)

	taskBody, err := json.Marshal(task)
	if err != nil {
		return err
	}

	wrapper := &Task{Type: taskType, OrgID: orgID, Task: taskBody, QueuedOn: dates.Now()}
	marshaled := jsonx.MustMarshal(wrapper)

	rc.Send("zadd", fmt.Sprintf(queuePattern, queue, orgID), score, marshaled)
	rc.Send("zincrby", fmt.Sprintf(activePattern, queue), 0, orgID)
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
	task := Task{}
	for {
		values, err := redis.Strings(popScript.Do(rc, queue))
		if err != nil {
			return nil, err
		}

		if values[0] == "empty" {
			return nil, nil
		}

		if values[0] == "retry" {
			continue
		}

		err = json.Unmarshal([]byte(values[1]), &task)
		return &task, err
	}
}

//go:embed lua/done.lua
var doneLua string
var doneScript = redis.NewScript(2, doneLua)

// Done marks the passed in task as complete. Callers must call this in order
// to maintain fair workers across orgs
func Done(rc redis.Conn, queue string, orgID int) error {
	_, err := doneScript.Do(rc, queue, strconv.FormatInt(int64(orgID), 10))
	return err
}
