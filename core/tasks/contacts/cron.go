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
	fires, err := models.LoadDueContactfires(ctx, rt)
	if err != nil {
		return nil, fmt.Errorf("error loading due contact fires: %w", err)
	}

	if len(fires) == 0 {
		return map[string]any{"fires": 0}, nil
	}

	ids := make([]models.ContactFireID, 0, 100)

	// queue as org batch tasks
	rc := rt.RP.Get()
	defer rc.Close()

	for orgID, orgFires := range fires {
		for batch := range slices.Chunk(orgFires, c.bulkBatchSize) {
			if err := tasks.Queue(rc, tasks.BatchQueue, orgID, &BulkFireTask{Fires: batch}, true); err != nil {
				return nil, fmt.Errorf("error queuing bulk fire task for org #%d: %w", orgID, err)
			}
		}
	}

	if err := models.DeleteContactFires(ctx, rt, ids); err != nil {
		return nil, fmt.Errorf("error deleting queued contact fires: %w", err)
	}

	return map[string]any{"fires": len(ids), "orgs": len(fires)}, nil
}
