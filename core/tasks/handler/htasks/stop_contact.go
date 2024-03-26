package htasks

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	handler.RegisterTaskType(string(models.EventTypeStopContact), func() handler.Task { return &StopContactTask{} })
}

type StopContactTask struct {
	CreatedOn time.Time `json:"created_on"`
}

func (t *StopContactTask) Type() string {
	return string(models.EventTypeStopContact)
}

func (t *StopContactTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, contactID models.ContactID) error {
	tx, err := rt.DB.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "unable to start transaction for stopping contact")
	}

	err = models.StopContact(ctx, tx, orgID, contactID)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = models.UpdateContactLastSeenOn(ctx, tx, contactID, t.CreatedOn)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrapf(err, "unable to commit for contact stop")
	}
	return err
}
