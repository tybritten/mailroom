package ctasks

import (
	"context"
	"log/slog"
	"time"

	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

const TypeWaitExpiration = "expiration_event"

func init() {
	handler.RegisterContactTask(TypeWaitExpiration, func() handler.Task { return &WaitExpirationTask{} })
}

type WaitExpirationTask struct {
	SessionID models.SessionID `json:"session_id"`
	Time      time.Time        `json:"time"`
}

func NewWaitExpiration(sessionID models.SessionID, time time.Time) *WaitExpirationTask {
	return &WaitExpirationTask{SessionID: sessionID, Time: time}
}

func (t *WaitExpirationTask) Type() string {
	return TypeWaitExpiration
}

func (t *WaitExpirationTask) UseReadOnly() bool {
	return true
}

func (t *WaitExpirationTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *models.Contact) error {
	// if contact is deleted, nothing to do
	if contact == nil {
		return nil
	}

	log := slog.With("contact_id", contact.ID(), "session_id", t.SessionID)

	// build our flow contact
	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// look for a waiting session for this contact
	session, err := models.FindWaitingSessionForContact(ctx, rt.DB, rt.SessionStorage, oa, models.FlowTypeMessaging, flowContact)
	if err != nil {
		return errors.Wrapf(err, "error loading waiting session for contact")
	}

	// if we didn't find a session or it is another session then this session has already been interrupted
	if session == nil || session.ID() != t.SessionID {
		return nil
	}

	// check that our expiration is still the same
	expiresOn, err := models.GetSessionWaitExpiresOn(ctx, rt.DB, t.SessionID)
	if err != nil {
		return errors.Wrapf(err, "unable to load expiration for run")
	}

	if expiresOn == nil {
		log.Info("ignoring expiration, session no longer waiting", "event_expiration", t.Time)
		return nil
	}

	if !expiresOn.Equal(t.Time) {
		log.Info("ignoring expiration, has been updated", "event_expiration", t.Time, "run_expiration", expiresOn)
		return nil
	}

	resume := resumes.NewRunExpiration(oa.Env(), flowContact)

	_, err = runner.ResumeFlow(ctx, rt, oa, session, contact, resume, nil)
	if err != nil {
		return errors.Wrap(err, "error resuming flow for expiration")
	}

	return nil
}
