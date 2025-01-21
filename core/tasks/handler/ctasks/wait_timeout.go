package ctasks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
)

const TypeWaitTimeout = "timeout_event"

func init() {
	handler.RegisterContactTask(TypeWaitTimeout, func() handler.Task { return &WaitTimeoutTask{} })
}

type WaitTimeoutTask struct {
	SessionID  models.SessionID `json:"session_id"`
	ModifiedOn time.Time        `json:"modified_on"` // session modified_on to check it hasn't been changed since we were queued
}

func NewWaitTimeout(sessionID models.SessionID, modifiedOn time.Time) *WaitTimeoutTask {
	return &WaitTimeoutTask{SessionID: sessionID, ModifiedOn: modifiedOn}
}

func (t *WaitTimeoutTask) Type() string {
	return TypeWaitTimeout
}

func (t *WaitTimeoutTask) UseReadOnly() bool {
	return true
}

func (t *WaitTimeoutTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *models.Contact) error {
	log := slog.With("contact_id", contact.ID(), "session_id", t.SessionID)

	// build our flow contact
	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return fmt.Errorf("error creating flow contact: %w", err)
	}

	// look for a waiting session for this contact
	session, err := models.FindWaitingSessionForContact(ctx, rt, oa, models.FlowTypeMessaging, flowContact)
	if err != nil {
		return fmt.Errorf("error loading waiting session for contact: %w", err)
	}

	// if we didn't find a session or it is another session or if it's been modified since, ignore this task
	if session == nil || session.ID() != t.SessionID || !session.ModifiedOn().Equal(t.ModifiedOn) {
		return nil
	}

	resume := resumes.NewWaitTimeout(oa.Env(), flowContact)

	_, err = runner.ResumeFlow(ctx, rt, oa, session, contact, resume, nil)
	if err != nil {
		// if we errored, and it's the wait rejecting the timeout event, it's because it no longer exists on the flow, so clear it
		// on the session
		var eerr *engine.Error
		if errors.As(err, &eerr) && eerr.Code() == engine.ErrorResumeRejectedByWait && resume.Type() == resumes.TypeWaitTimeout {
			log.Info("clearing session timeout which is no longer set in flow")

			if err := session.ClearWaitTimeout(ctx, rt.DB); err != nil {
				return fmt.Errorf("error clearing session timeout: %w", err)
			}

			return nil
		}

		return fmt.Errorf("error resuming flow for timeout: %w", err)
	}

	return nil
}
