package contacts

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	tasks.RegisterCron("contact_fires", &FiresCron{fetchBatchSize: 10_000, taskBatchSize: 100})
}

type FiresCron struct {
	fetchBatchSize int
	taskBatchSize  int
}

func NewFiresCron(fetchBatchSize, taskBatchSize int) *FiresCron {
	return &FiresCron{fetchBatchSize: fetchBatchSize, taskBatchSize: taskBatchSize}
}

func (c *FiresCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

func (c *FiresCron) AllInstances() bool {
	return false
}

func (c *FiresCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	start := time.Now()
	numExpires, numHangups, numTimeouts := 0, 0, 0

	rc := rt.RP.Get()
	defer rc.Close()

	for {
		fires, err := models.LoadDueContactfires(ctx, rt, c.fetchBatchSize)
		if err != nil {
			return nil, fmt.Errorf("error loading due contact fires: %w", err)
		}
		if len(fires) == 0 {
			break
		}

		// organize fires by org and type
		expirations := make(map[models.OrgID][]*models.ContactFire, 100)
		hangups := make(map[models.OrgID][]*models.ContactFire, 100)
		timeouts := make(map[models.OrgID][]*models.ContactFire, 100)

		for _, f := range fires {
			if f.Type == models.ContactFireTypeWaitExpiration {
				if f.Extra.V.CallID == models.NilCallID {
					expirations[f.OrgID] = append(expirations[f.OrgID], f)
				} else {
					hangups[f.OrgID] = append(hangups[f.OrgID], f)
				}
			} else if f.Type == models.ContactFireTypeWaitTimeout {
				timeouts[f.OrgID] = append(timeouts[f.OrgID], f)
			} else if f.Type == models.ContactFireTypeCampaign {
				// TODO
			}
		}

		// turn expires into bulk expire tasks
		for orgID, orgExpires := range expirations {
			for batch := range slices.Chunk(orgExpires, c.taskBatchSize) {
				es := make([]*Expiration, len(batch))
				for i, f := range batch {
					es[i] = &Expiration{ContactID: f.ContactID, SessionID: f.Extra.V.SessionID, ModifiedOn: f.Extra.V.SessionModifiedOn}
				}

				// put expirations in throttled queue but high priority so they get priority over flow starts etc
				if err := tasks.Queue(rc, tasks.ThrottledQueue, orgID, &BulkSessionExpireTask{Expirations: es}, true); err != nil {
					return nil, fmt.Errorf("error queuing bulk fire task for org #%d: %w", orgID, err)
				}
				numExpires += len(batch)

				if err := models.DeleteContactFires(ctx, rt, batch); err != nil {
					return nil, fmt.Errorf("error deleting queued contact fires: %w", err)
				}
			}
		}

		// turn voice expires into bulk hangup tasks
		for orgID, orgHangups := range hangups {
			for batch := range slices.Chunk(orgHangups, c.taskBatchSize) {
				hs := make([]*ivr.Hangup, len(batch))
				for i, f := range batch {
					hs[i] = &ivr.Hangup{SessionID: f.Extra.V.SessionID, CallID: f.Extra.V.CallID}
				}

				// put hangups in batch queue but high priority so they get priority over imports etc
				if err := tasks.Queue(rc, tasks.BatchQueue, orgID, &ivr.BulkCallHangupTask{Hangups: hs}, true); err != nil {
					return nil, fmt.Errorf("error queuing bulk fire task for org #%d: %w", orgID, err)
				}
				numHangups += len(batch)

				if err := models.DeleteContactFires(ctx, rt, batch); err != nil {
					return nil, fmt.Errorf("error deleting queued contact fires: %w", err)
				}
			}
		}

		// turn timeouts into bulk timeout tasks
		for orgID, orgTimeouts := range timeouts {
			for batch := range slices.Chunk(orgTimeouts, c.taskBatchSize) {
				ts := make([]*Timeout, len(batch))
				for i, f := range batch {
					ts[i] = &Timeout{ContactID: f.ContactID, SessionID: f.Extra.V.SessionID, ModifiedOn: f.Extra.V.SessionModifiedOn}
				}

				// put timeouts in throttled queue but high priority so they get priority over flow starts etc
				if err := tasks.Queue(rc, tasks.ThrottledQueue, orgID, &BulkSessionTimeoutTask{Timeouts: ts}, true); err != nil {
					return nil, fmt.Errorf("error queuing bulk fire task for org #%d: %w", orgID, err)
				}
				numTimeouts += len(batch)

				if err := models.DeleteContactFires(ctx, rt, batch); err != nil {
					return nil, fmt.Errorf("error deleting queued contact fires: %w", err)
				}
			}
		}

		// if we're getting close to a minute, stop and let the next cron run handle the rest
		if time.Since(start) > 50*time.Second {
			break
		}
	}

	return map[string]any{"expires": numExpires, "hangups": numHangups, "timeouts": numTimeouts}, nil
}
