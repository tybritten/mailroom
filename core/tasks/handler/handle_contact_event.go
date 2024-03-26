package handler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/queues"
	"github.com/pkg/errors"
)

// TypeHandleContactEvent is the task type for flagging that a contact has handler tasks to be handled
const TypeHandleContactEvent = "handle_contact_event"

func init() {
	tasks.RegisterType(TypeHandleContactEvent, func() tasks.Task { return &HandleContactEventTask{} })
}

// HandleContactEventTask is the task to flag that a contact has tasks
type HandleContactEventTask struct {
	ContactID models.ContactID `json:"contact_id"`
}

func (t *HandleContactEventTask) Type() string {
	return TypeHandleContactEvent
}

// Timeout is the maximum amount of time the task can run for
func (t *HandleContactEventTask) Timeout() time.Duration {
	return time.Minute * 5
}

// Perform is called when an event comes in for a contact. To make sure we don't get into a situation of being off by one,
// this task ingests and handles all the events for a contact, one by one.
func (t *HandleContactEventTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	// try to get the lock for this contact, waiting up to 10 seconds
	locks, _, err := models.LockContacts(ctx, rt, orgID, []models.ContactID{t.ContactID}, time.Second*10)
	if err != nil {
		return errors.Wrapf(err, "error acquiring lock for contact %d", t.ContactID)
	}

	// we didn't get the lock.. requeue for later
	if len(locks) == 0 {
		rc := rt.RP.Get()
		defer rc.Close()
		err = tasks.Queue(rc, tasks.HandlerQueue, orgID, &HandleContactEventTask{ContactID: t.ContactID}, queues.DefaultPriority)
		if err != nil {
			return errors.Wrapf(err, "error re-adding contact task after failing to get lock")
		}
		slog.Info("failed to get lock for contact, requeued and skipping", "org_id", orgID, "contact_id", t.ContactID)
		return nil
	}

	defer models.UnlockContacts(rt, orgID, locks)

	// read all the events for this contact, one by one
	contactQ := fmt.Sprintf("c:%d:%d", orgID, t.ContactID)
	for {
		// pop the next event off this contacts queue
		rc := rt.RP.Get()
		event, err := redis.String(rc.Do("lpop", contactQ))
		rc.Close()

		// out of tasks? that's ok, exit
		if err == redis.ErrNil {
			return nil
		}

		// real error? report
		if err != nil {
			return errors.Wrapf(err, "error popping handler task")
		}

		start := time.Now()

		// decode our event, this is a normal task at its top level
		taskPayload := &payload{}
		jsonx.MustUnmarshal([]byte(event), taskPayload)

		htask, err := readTask(taskPayload.Type, taskPayload.Task)
		if err != nil {
			return errors.Wrapf(err, "error reading handler task")
		}

		err = htask.Perform(ctx, rt, orgID, t.ContactID)

		// log our processing time to librato
		analytics.Gauge(fmt.Sprintf("mr.%s_elapsed", taskPayload.Type), float64(time.Since(start))/float64(time.Second))

		// and total latency for this task since it was queued
		analytics.Gauge(fmt.Sprintf("mr.%s_latency", taskPayload.Type), float64(time.Since(taskPayload.QueuedOn))/float64(time.Second))

		// if we get an error processing an event, requeue it for later and return our error
		if err != nil {
			log := slog.With("org_id", orgID, "contact_id", t.ContactID, "event", event)

			if qerr := dbutil.AsQueryError(err); qerr != nil {
				query, params := qerr.Query()
				log = log.With("sql", query, "sql_params", params)
			}

			taskPayload.ErrorCount++
			if taskPayload.ErrorCount < 3 {
				rc := rt.RP.Get()
				retryErr := queueTask(rc, orgID, t.ContactID, htask, true, taskPayload.ErrorCount)
				if retryErr != nil {
					slog.Error("error requeuing errored contact event", "error", retryErr)
				}
				rc.Close()

				log.Error("error handling contact event", "error", err, "error_count", taskPayload.ErrorCount)
				return nil
			}
			log.Error("error handling contact event, permanent failure", "error", err)
			return nil
		}
	}
}

type DBHook func(ctx context.Context, tx *sqlx.Tx) error

// TriggerIVRFlow will create a new flow start with the passed in flow and set of contacts. This will cause us to
// request calls to start, which once we get the callback will trigger our actual flow to start.
func TriggerIVRFlow(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, flowID models.FlowID, contactIDs []models.ContactID, hook DBHook) error {
	tx, _ := rt.DB.BeginTxx(ctx, nil)

	// create and insert our flow start
	start := models.NewFlowStart(orgID, models.StartTypeTrigger, flowID).WithContactIDs(contactIDs)
	err := models.InsertFlowStarts(ctx, tx, []*models.FlowStart{start})
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error inserting ivr flow start")
	}

	// call our hook if we have one
	if hook != nil {
		err = hook(ctx, tx)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error while calling db hook")
		}
	}

	// commit our transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error committing transaction for ivr flow starts")
	}

	// create our batch of all our contacts
	task := &ivr.StartIVRFlowBatchTask{FlowStartBatch: start.CreateBatch(contactIDs, models.FlowTypeVoice, true, len(contactIDs))}

	// queue this to our ivr starter, it will take care of creating the calls then calling back in
	rc := rt.RP.Get()
	defer rc.Close()
	err = tasks.Queue(rc, tasks.BatchQueue, orgID, task, queues.HighPriority)
	if err != nil {
		return errors.Wrapf(err, "error queuing ivr flow start")
	}

	return nil
}
