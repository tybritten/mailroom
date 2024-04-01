package android

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/gocommon/stringsx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/ctasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/android/message", web.RequireAuthToken(web.JSONPayload(handleMessage)))
}

// Creates a new incoming message from an Android relayer sync.
//
//	{
//	  "org_id": 1,
//	  "channel_id": 12,
//	  "urn": "tel:+250788123123",
//	  "text": "Hello world",
//	  "received_on": "2021-01-01T12:00:00Z"
//	}
type messageRequest struct {
	OrgID      models.OrgID     `json:"org_id"       validate:"required"`
	ChannelID  models.ChannelID `json:"channel_id"   validate:"required"`
	URN        urns.URN         `json:"urn"          validate:"required"`
	Text       string           `json:"text"         validate:"required"`
	ReceivedOn time.Time        `json:"received_on"  validate:"required"`
}

func handleMessage(ctx context.Context, rt *runtime.Runtime, r *messageRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	cu, err := resolveContact(ctx, rt, oa, r.ChannelID, r.URN)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error resolving contact")
	}

	text := dbutil.ToValidUTF8(stringsx.Truncate(r.Text, 640))

	existingID, err := checkDuplicate(ctx, rt, text, cu.contactID, r.ReceivedOn)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error checking for duplicate message")
	}
	if existingID != models.NilMsgID {
		return map[string]any{"id": existingID, "duplicate": true}, http.StatusOK, nil
	}

	m := models.NewIncomingAndroid(r.OrgID, r.ChannelID, cu.contactID, cu.urnID, text, r.ReceivedOn)
	if err := models.InsertMessages(ctx, rt.DB, []*models.Msg{m}); err != nil {
		return nil, 0, errors.Wrap(err, "error inserting message")
	}

	rc := rt.RP.Get()
	defer rc.Close()

	err = handler.QueueTask(rc, r.OrgID, m.ContactID(), &ctasks.MsgEventTask{
		ChannelID:     m.ChannelID(),
		MsgID:         m.ID(),
		MsgUUID:       m.UUID(),
		MsgExternalID: m.ExternalID(),
		URN:           cu.urn,
		URNID:         *m.ContactURNID(),
		Text:          m.Text(),
		NewContact:    cu.newContact,
	})
	if err != nil {
		return nil, 0, errors.Wrap(err, "error queueing handle task")
	}

	return map[string]any{"id": m.ID(), "duplicate": false}, http.StatusOK, nil
}

func checkDuplicate(ctx context.Context, rt *runtime.Runtime, text string, contactID models.ContactID, sentOn time.Time) (models.MsgID, error) {
	row := rt.DB.QueryRowContext(ctx, `SELECT id FROM msgs_msg WHERE direction = 'I' AND text = $1 AND contact_id = $2 AND sent_on = $3 LIMIT 1`, text, contactID, sentOn)

	var id models.MsgID
	err := row.Scan(&id)
	if err != nil && err != sql.ErrNoRows {
		return models.NilMsgID, errors.Wrap(err, "error checking for duplicate message")
	}

	return id, nil
}
