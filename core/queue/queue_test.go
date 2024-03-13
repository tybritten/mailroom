package queue_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueues(t *testing.T) {
	_, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2022, 1, 1, 12, 1, 2, 123456789, time.UTC)))
	defer dates.SetNowSource(dates.DefaultNowSource)

	defer testsuite.Reset(testsuite.ResetRedis)

	assertPop := func(expected string) {
		task, err := queue.Pop(rc, "test")
		require.NoError(t, err)
		if expected != "" {
			assert.Equal(t, expected, string(task.Task))
		} else {
			assert.Nil(t, task)
		}
	}

	queue.Push(rc, "test", "type1", 1, "task1", queue.DefaultPriority)
	queue.Push(rc, "test", "type1", 1, "task2", queue.HighPriority)
	queue.Push(rc, "test", "type1", 2, "task3", queue.LowPriority)
	queue.Push(rc, "test", "type2", 1, "task4", queue.DefaultPriority)
	queue.Push(rc, "test", "type2", 2, "task5", queue.DefaultPriority)

	// nobody processing any tasks so no workers assigned in active set
	assertredis.ZGetAll(t, rt.RP, "test:active", map[string]float64{"1": 0, "2": 0})

	assertredis.ZGetAll(t, rt.RP, "test:1", map[string]float64{
		`{"type":"type1","org_id":1,"task":"task2","queued_on":"2022-01-01T12:01:05.123456789Z"}`: 1631038464.123456,
		`{"type":"type1","org_id":1,"task":"task1","queued_on":"2022-01-01T12:01:03.123456789Z"}`: 1641038462.123456,
		`{"type":"type2","org_id":1,"task":"task4","queued_on":"2022-01-01T12:01:09.123456789Z"}`: 1641038468.123456,
	})
	assertredis.ZGetAll(t, rt.RP, "test:2", map[string]float64{
		`{"type":"type1","org_id":2,"task":"task3","queued_on":"2022-01-01T12:01:07.123456789Z"}`: 1651038466.123456,
		`{"type":"type2","org_id":2,"task":"task5","queued_on":"2022-01-01T12:01:11.123456789Z"}`: 1641038470.123456,
	})

	assertPop(`"task2"`) // because it's highest priority for owner 1
	assertredis.ZGetAll(t, rt.RP, "test:active", map[string]float64{"1": 1, "2": 0})
	assertPop(`"task5"`) // because it's highest priority for owner 2
	assertredis.ZGetAll(t, rt.RP, "test:active", map[string]float64{"1": 1, "2": 1})
	assertPop(`"task1"`)
	assertredis.ZGetAll(t, rt.RP, "test:active", map[string]float64{"1": 2, "2": 1})

	// mark task2 and task1 (owner 1) as complete
	queue.Done(rc, "test", 1)
	queue.Done(rc, "test", 1)

	assertredis.ZGetAll(t, rt.RP, "test:active", map[string]float64{"1": 0, "2": 1})

	assertPop(`"task4"`)
	assertPop(`"task3"`)
	assertPop("") // no more tasks

	assertredis.ZGetAll(t, rt.RP, "test:active", map[string]float64{})

	//  if we somehow get into a state where an owner is in the active set but doesn't have queued tasks, pop will retry
	queue.Push(rc, "test", "type1", 1, "task6", queue.DefaultPriority)
	queue.Push(rc, "test", "type1", 2, "task7", queue.DefaultPriority)

	rc.Do("ZREMRANGEBYRANK", "test:1", 0, 1)

	assertPop(`"task7"`)
	assertPop("")

	// if we somehow call done too many times, we never get negative workers
	queue.Push(rc, "test", "type1", 1, "task8", queue.DefaultPriority)
	queue.Done(rc, "test", 1)
	queue.Done(rc, "test", 1)

	assertredis.ZGetAll(t, rt.RP, "test:active", map[string]float64{"1": 0})

}
