package htasks

import (
	"context"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

const TypeMsgDeleted = "msg_deleted"

func init() {
	handler.RegisterTaskType(TypeMsgDeleted, func() handler.Task { return &MsgDeletedTask{} })
}

type MsgDeletedTask struct {
	MsgID models.MsgID `json:"message_id"`
}

func (t *MsgDeletedTask) Type() string {
	return TypeMsgDeleted
}

func (t *MsgDeletedTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, contactID models.ContactID) error {
	err := models.UpdateMessageDeletedBySender(ctx, rt.DB.DB, orgID, t.MsgID)
	return errors.Wrap(err, "error deleting message")
}
