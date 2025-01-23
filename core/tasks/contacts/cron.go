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
	numExpires, numTimeouts, numCampaigns := 0, 0, 0

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

		type orgAndType struct {
			orgID models.OrgID
			typ   models.ContactFireType
		}

		// organize tasks by org and type
		byOrgAndType := make(map[orgAndType][]*models.ContactFire)
		for _, f := range fires {
			byOrgAndType[orgAndType{f.OrgID, f.Type}] = append(byOrgAndType[orgAndType{f.OrgID, f.Type}], f)
		}

		for ot, fs := range byOrgAndType {
			for batch := range slices.Chunk(fs, c.taskBatchSize) {
				if ot.typ == models.ContactFireTypeWaitExpiration {
					es := make([]*Expiration, len(batch))
					for i, f := range batch {
						es[i] = &Expiration{ContactID: f.ContactID, SessionID: f.Extra.V.SessionID, ModifiedOn: f.Extra.V.SessionModifiedOn}
					}

					// put expirations in throttled queue but high priority so they get priority over flow starts etc
					if err := tasks.Queue(rc, tasks.ThrottledQueue, ot.orgID, &BulkSessionExpireTask{Expirations: es}, true); err != nil {
						return nil, fmt.Errorf("error queuing bulk fire task for org #%d: %w", ot.orgID, err)
					}
					numExpires += len(batch)

				} else if ot.typ == models.ContactFireTypeWaitTimeout {
					ts := make([]*Timeout, len(batch))
					for i, f := range batch {
						ts[i] = &Timeout{ContactID: f.ContactID, SessionID: f.Extra.V.SessionID, ModifiedOn: f.Extra.V.SessionModifiedOn}
					}

					// put timeouts in throttled queue but high priority so they get priority over flow starts etc
					if err := tasks.Queue(rc, tasks.ThrottledQueue, ot.orgID, &BulkSessionTimeoutTask{Timeouts: ts}, true); err != nil {
						return nil, fmt.Errorf("error queuing bulk fire task for org #%d: %w", ot.orgID, err)
					}
					numTimeouts += len(batch)

				} else if ot.typ == models.ContactFireTypeCampaign {
					// TODO

					numCampaigns += len(batch)
				}

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

	return map[string]any{"expires": numExpires, "timeouts": numTimeouts, "campaigns": numCampaigns}, nil
}
