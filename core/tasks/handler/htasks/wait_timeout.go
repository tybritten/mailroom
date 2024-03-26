package htasks

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

const TypeWaitTimeout = "timeout_event"

func init() {
	handler.RegisterTaskType(TypeWaitTimeout, func() handler.Task { return &WaitTimeoutTask{} })
}

type WaitTimeoutTask struct {
	SessionID models.SessionID `json:"session_id"`
	Time      time.Time        `json:"time"`
}

func NewWaitTimeout(sessionID models.SessionID, time time.Time) *WaitTimeoutTask {
	return &WaitTimeoutTask{SessionID: sessionID, Time: time}
}

func (t *WaitTimeoutTask) Type() string {
	return TypeWaitTimeout
}

func (t *WaitTimeoutTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	start := time.Now()
	log := slog.With("event_type", t.Type(), "contact_id", contactID, "session_id", t.SessionID)

	// load our contact
	modelContact, err := models.LoadContact(ctx, rt.ReadonlyDB, oa, contactID)
	if err != nil {
		if err == sql.ErrNoRows { // if contact no longer exists, ignore event
			return nil
		}
		return errors.Wrapf(err, "error loading contact")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// look for a waiting session for this contact
	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, contact)
	if err != nil {
		return errors.Wrapf(err, "error loading waiting session for contact")
	}

	// if we didn't find a session or it is another session then this session has already been interrupted
	if session == nil || session.ID() != t.SessionID {
		return nil
	}

	if session.WaitTimeoutOn() == nil {
		log.Info("ignoring session timeout, has no timeout set", "session_id", session.ID())
		return nil
	}

	// check that the timeout is the same
	timeout := *session.WaitTimeoutOn()
	if !timeout.Equal(t.Time) {
		log.Info("ignoring timeout, has been updated", "event_timeout", t.Time, "session_timeout", timeout)
		return nil
	}

	resume := resumes.NewWaitTimeout(oa.Env(), contact)

	_, err = runner.ResumeFlow(ctx, rt, oa, session, modelContact, resume, nil)
	if err != nil {
		// if we errored, and it's the wait rejecting the timeout event, it's because it no longer exists on the flow, so clear it
		// on the session
		var eerr *engine.Error
		if errors.As(err, &eerr) && eerr.Code() == engine.ErrorResumeRejectedByWait && resume.Type() == resumes.TypeWaitTimeout {
			log.Info("clearing session timeout which is no longer set in flow", "session_id", session.ID())
			return errors.Wrap(session.ClearWaitTimeout(ctx, rt.DB), "error clearing session timeout")
		}

		return errors.Wrap(err, "error resuming flow for timeout")
	}

	log.Info("handled timed event", "elapsed", time.Since(start))
	return nil
}
