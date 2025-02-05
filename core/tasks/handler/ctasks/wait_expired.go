package ctasks

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
)

const TypeWaitExpired = "wait_expired"

func init() {
	handler.RegisterContactTask(TypeWaitExpired, func() handler.Task { return &WaitExpiredTask{} })
	handler.RegisterContactTask("expiration_event", func() handler.Task { return &WaitExpiredTask{} }) // legacy
}

type WaitExpiredTask struct {
	SessionUUID flows.SessionUUID `json:"session_uuid"`
	SprintUUID  flows.SprintUUID  `json:"sprint_uuid"`
}

func (t *WaitExpiredTask) Type() string {
	return TypeWaitExpired
}

func (t *WaitExpiredTask) UseReadOnly() bool {
	return true
}

func (t *WaitExpiredTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *models.Contact) error {
	log := slog.With("ctask", "wait_expired", "contact_id", contact.ID(), "session_uuid", t.SessionUUID)

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
	if session == nil || session.UUID() != t.SessionUUID {
		log.Debug("skipping as waiting session has changed")
		return nil
	}
	if session.LastSprintUUID() != t.SprintUUID {
		log.Debug("skipping as session has been modified since", "session_sprint", session.LastSprintUUID(), "task_sprint", t.SprintUUID)
		return nil
	}

	resume := resumes.NewRunExpiration(oa.Env(), flowContact)

	_, err = runner.ResumeFlow(ctx, rt, oa, session, contact, resume, nil)
	if err != nil {
		return fmt.Errorf("error resuming flow for expiration: %w", err)
	}

	return nil
}
