package msg

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/ctasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/msg/handle", web.RequireAuthToken(web.JSONPayload(handleHandle)))
}

// Queues the given incoming messages for handling. This is only used for recovering from failures where we might need
// to manually retry handling of a message.
//
//	{
//	  "org_id": 1,
//	  "msg_ids": [12345, 23456]
//	}
type handleRequest struct {
	OrgID  models.OrgID   `json:"org_id"  validate:"required"`
	MsgIDs []models.MsgID `json:"msg_ids" validate:"required"`
}

// handles a request to resend the given messages
func handleHandle(ctx context.Context, rt *runtime.Runtime, r *handleRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	msgs, err := models.GetMessagesByID(ctx, rt.DB, oa.OrgID(), models.DirectionIn, r.MsgIDs)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error loading messages to handle")
	}

	rc := rt.RP.Get()
	defer rc.Close()

	// response is the ids of the messages that were actually queued
	queuedMsgIDs := make([]models.MsgID, 0, len(r.MsgIDs))

	for _, m := range msgs {
		if m.Status() != models.MsgStatusPending || m.ContactURNID() == nil {
			continue
		}

		urn, err := models.URNForID(ctx, rt.DB, oa, *m.ContactURNID())
		if err != nil {
			return nil, 0, errors.Wrap(err, "error fetching msg URN")
		}

		attachments := make([]string, len(m.Attachments()))
		for i := range m.Attachments() {
			attachments[i] = string(m.Attachments()[i])
		}

		err = handler.QueueTask(rc, m.OrgID(), m.ContactID(), &ctasks.MsgEventTask{
			ChannelID:     m.ChannelID(),
			MsgID:         m.ID(),
			MsgUUID:       m.UUID(),
			MsgExternalID: m.ExternalID(),
			URN:           urn,
			URNID:         *m.ContactURNID(),
			Text:          m.Text(),
			Attachments:   attachments,
			NewContact:    false,
		})
		if err != nil {
			return nil, 0, errors.Wrap(err, "error queueing handle task")
		}

		queuedMsgIDs = append(queuedMsgIDs, m.ID())
	}

	return map[string]any{"msg_ids": queuedMsgIDs}, http.StatusOK, nil
}
