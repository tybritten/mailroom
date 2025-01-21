package contacts

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	tasks.RegisterCron("contact_fires", &firesCron{bulkBatchSize: 100})
}

type firesCron struct {
	bulkBatchSize int
}

func (c *firesCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

func (c *firesCron) AllInstances() bool {
	return false
}

func (c *firesCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	numFired := 0

	rc := rt.RP.Get()
	defer rc.Close()

	for {
		fires, err := models.LoadDueContactfires(ctx, rt)
		if err != nil {
			return nil, fmt.Errorf("error loading due contact fires: %w", err)
		}
		if len(fires) == 0 {
			break
		}

		for orgID, orgFires := range fires {
			for batch := range slices.Chunk(orgFires, c.bulkBatchSize) {
				// put fires in throttled queue but high priority so they get priority over flow starts etc
				if err := tasks.Queue(rc, tasks.ThrottledQueue, orgID, &BulkFireTask{Fires: batch}, true); err != nil {
					return nil, fmt.Errorf("error queuing bulk fire task for org #%d: %w", orgID, err)
				}

				if err := models.DeleteContactFires(ctx, rt, batch); err != nil {
					return nil, fmt.Errorf("error deleting queued contact fires: %w", err)
				}
			}
		}
	}

	return map[string]any{"fired": numFired}, nil
}
