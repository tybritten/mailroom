package expirations

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/redisx"
)

func init() {
	tasks.RegisterCron("run_expirations", NewExpirationsCron(100))
}

type ExpirationsCron struct {
	marker        *redisx.IntervalSet
	bulkBatchSize int // number of expirations to queue in a single bulk task
}

func NewExpirationsCron(bulkBatchSize int) *ExpirationsCron {
	return &ExpirationsCron{
		marker:        redisx.NewIntervalSet("run_expirations", time.Hour*24, 2),
		bulkBatchSize: bulkBatchSize,
	}
}

func (c *ExpirationsCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

func (c *ExpirationsCron) AllInstances() bool {
	return false
}

// handles waiting messaging sessions whose waits have expired
func (c *ExpirationsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	rows, err := rt.DB.QueryxContext(ctx, sqlSelectExpiredWaits)
	if err != nil {
		return nil, fmt.Errorf("error querying sessions with expired waits: %w", err)
	}
	defer rows.Close()

	taskID := func(w *ExpiredWait) string {
		return fmt.Sprintf("%d:%s", w.SessionID, w.WaitExpiresOn.Format(time.RFC3339))
	}

	// scan and organize by org
	byOrg := make(map[models.OrgID][]*ExpiredWait, 50)
	callsByOrg := make(map[models.OrgID][]*ExpiredWait, 50)

	rc := rt.RP.Get()
	defer rc.Close()

	numDupes, numExpires, numHangups := 0, 0, 0

	for rows.Next() {
		expiredWait := &ExpiredWait{}
		if err := rows.StructScan(expiredWait); err != nil {
			return nil, fmt.Errorf("error scanning expired wait: %w", err)
		}

		// check whether we've already queued this
		queued, err := c.marker.IsMember(rc, taskID(expiredWait))
		if err != nil {
			return nil, fmt.Errorf("error checking whether expiration is already queued: %w", err)
		}

		// already queued? move on
		if queued {
			numDupes++
			continue
		}

		if expiredWait.CallID != models.NilCallID {
			callsByOrg[expiredWait.OrgID] = append(callsByOrg[expiredWait.OrgID], expiredWait)
		} else {
			byOrg[expiredWait.OrgID] = append(byOrg[expiredWait.OrgID], expiredWait)
		}
	}

	for orgID, expirations := range byOrg {
		for batch := range slices.Chunk(expirations, c.bulkBatchSize) {
			exps := make([]*contacts.Expiration, len(batch))
			for i, exp := range batch {
				exps[i] = &contacts.Expiration{ContactID: exp.ContactID, SessionID: exp.SessionID, ModifiedOn: exp.ModifiedOn}
			}

			if err := tasks.Queue(rc, tasks.ThrottledQueue, orgID, &contacts.BulkSessionExpireTask{Expirations: exps}, true); err != nil {
				return nil, fmt.Errorf("error queuing bulk expiration task to throttle queue: %w", err)
			}
			numExpires += len(batch)

			for _, exp := range batch {
				// mark as queued
				if err = c.marker.Add(rc, taskID(exp)); err != nil {
					return nil, fmt.Errorf("error marking expiration task as queued: %w", err)
				}
			}
		}
	}

	for orgID, expirations := range callsByOrg {
		for batch := range slices.Chunk(expirations, c.bulkBatchSize) {
			hups := make([]*ivr.Hangup, len(batch))
			for i, exp := range batch {
				hups[i] = &ivr.Hangup{SessionID: exp.SessionID, CallID: exp.CallID}
			}

			if err := tasks.Queue(rc, tasks.BatchQueue, orgID, &ivr.BulkCallHangupTask{Hangups: hups}, true); err != nil {
				return nil, fmt.Errorf("error queuing bulk hangup task to batch queue: %w", err)
			}
			numHangups += len(batch)

			for _, exp := range batch {
				// mark as queued
				if err = c.marker.Add(rc, taskID(exp)); err != nil {
					return nil, fmt.Errorf("error marking hangup task as queued: %w", err)
				}
			}
		}
	}

	return map[string]any{"dupes": numDupes, "queued_expires": numExpires, "queued_hangups": numHangups}, nil
}

const sqlSelectExpiredWaits = `
    SELECT id, org_id, contact_id, call_id, wait_expires_on, modified_on
      FROM flows_flowsession
     WHERE status = 'W' AND wait_expires_on <= NOW()
  ORDER BY wait_expires_on ASC
     LIMIT 25000`

type ExpiredWait struct {
	SessionID     models.SessionID `db:"id"`
	OrgID         models.OrgID     `db:"org_id"`
	ContactID     models.ContactID `db:"contact_id"`
	CallID        models.CallID    `db:"call_id"`
	WaitExpiresOn time.Time        `db:"wait_expires_on"`
	ModifiedOn    time.Time        `db:"modified_on"`
}
