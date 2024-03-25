package testsuite

import (
	"context"
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/queues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func CurrentTasks(t *testing.T, rt *runtime.Runtime) map[models.OrgID][]*queues.Task {
	rc := rt.RP.Get()
	defer rc.Close()

	// get all active org queues
	active, err := redis.Ints(rc.Do("ZRANGE", "batch:active", 0, -1))
	require.NoError(t, err)

	tasks := make(map[models.OrgID][]*queues.Task)
	for _, orgID := range active {
		orgTasksEncoded, err := redis.Strings(rc.Do("ZRANGE", fmt.Sprintf("batch:%d", orgID), 0, -1))
		require.NoError(t, err)

		orgTasks := make([]*queues.Task, len(orgTasksEncoded))

		for i := range orgTasksEncoded {
			task := &queues.Task{}
			jsonx.MustUnmarshal([]byte(orgTasksEncoded[i]), task)
			orgTasks[i] = task
		}

		tasks[models.OrgID(orgID)] = orgTasks
	}

	return tasks
}

func FlushTasks(t *testing.T, rt *runtime.Runtime) map[string]int {
	rc := rt.RP.Get()
	defer rc.Close()

	var task *queues.Task
	var err error
	counts := make(map[string]int)

	for {
		// look for a task on the handler queue
		task, err = tasks.HandlerQueue.Pop(rc)
		require.NoError(t, err)

		if task == nil {
			// look for a task on the batch queue
			task, err = tasks.BatchQueue.Pop(rc)
			require.NoError(t, err)
		}

		if task == nil { // all done
			break
		}

		counts[task.Type]++

		err = tasks.Perform(context.Background(), rt, task)
		assert.NoError(t, err)
	}
	return counts
}
