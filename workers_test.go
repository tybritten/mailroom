package mailroom_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/queue"
)

type testTask struct{}

func (t *testTask) Type() string           { return "test" }
func (t *testTask) Timeout() time.Duration { return 5 * time.Second }
func (t *testTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	time.Sleep(100 * time.Millisecond)
	return nil
}

func TestFoo(t *testing.T) {
	_, rt := testsuite.Runtime()
	wg := &sync.WaitGroup{}
	q := queue.NewFair("test")

	rc := rt.RP.Get()
	defer rc.Close()

	tasks.RegisterType("test", func() tasks.Task { return &testTask{} })

	// queue up tasks of unknown type to ensure it doesn't break further processing
	q.Push(rc, "spam", 1, "argh", queue.DefaultPriority)
	q.Push(rc, "spam", 2, "argh", queue.DefaultPriority)

	// queue up 5 tasks for two orgs
	for range 5 {
		q.Push(rc, "test", 1, &testTask{}, queue.DefaultPriority)
	}
	for range 5 {
		q.Push(rc, "test", 2, &testTask{}, queue.DefaultPriority)
	}

	fm := mailroom.NewForeman(rt, wg, q, 2)
	fm.Start()

	// wait for queue to empty
	for {
		if size, err := q.Size(rc); err != nil || size == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	fm.Stop()
	wg.Wait()
}
