package starts

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/queues"
)

const (
	outboxThreshold = 10_000
)

func init() {
	tasks.RegisterCron("trottle_queue", &ThrottleQueueCron{queue: tasks.StartsQueue})
}

type ThrottleQueueCron struct {
	queue *queues.FairSorted
}

func (c *ThrottleQueueCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Second*10)
}

func (c *ThrottleQueueCron) AllInstances() bool {
	return false
}

// Run throttles processing of starts based on that org's current outbox size
func (c *ThrottleQueueCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	rc := rt.RP.Get()
	defer rc.Close()

	owners, err := c.queue.Owners(rc)
	if err != nil {
		return nil, fmt.Errorf("error getting task owners: %w", err)
	}

	numPaused, numResumed := 0, 0

	for ownerID := range owners {
		oa, err := models.GetOrgAssets(ctx, rt, models.OrgID(ownerID))
		if err != nil {
			return nil, fmt.Errorf("error org assets for org #%d: %w", ownerID, err)
		}

		if oa.Org().OutboxCount() >= outboxThreshold {
			c.queue.Pause(rc, ownerID)
			numPaused++
		} else {
			c.queue.Resume(rc, ownerID)
			numResumed++
		}
	}

	return map[string]any{"paused": numPaused, "resumed": numResumed}, nil
}
