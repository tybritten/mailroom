package android

import (
	"context"
	"net/http"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/ctasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/android/event", web.RequireAuthToken(web.JSONPayload(handleEvent)))
}

// Creates a new channel event from an Android relayer sync.
//
//	{
//	  "org_id": 1,
//	  "channel_id": 12,
//	  "urn": "tel:+250788123123",
//	  "event_type": "mo_miss",
//	  "extra": {"duration": 3},
//	  "occurred_on": "2021-01-01T12:00:00Z"
//	}
type eventRequest struct {
	OrgID      models.OrgID            `json:"org_id"       validate:"required"`
	ChannelID  models.ChannelID        `json:"channel_id"   validate:"required"`
	URN        urns.URN                `json:"urn"          validate:"required"`
	EventType  models.ChannelEventType `json:"event_type"   validate:"required"`
	Extra      null.Map[any]           `json:"extra"        validate:"required"`
	OccurredOn time.Time               `json:"occurred_on"  validate:"required"`
}

func handleEvent(ctx context.Context, rt *runtime.Runtime, r *eventRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	cu, err := resolveContact(ctx, rt, oa, r.ChannelID, r.URN)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error resolving contact")
	}

	e := models.NewChannelEvent(r.EventType, r.OrgID, r.ChannelID, cu.contactID, cu.urnID, r.Extra, r.OccurredOn)
	if err := e.Insert(ctx, rt.DB); err != nil {
		return nil, 0, errors.Wrap(err, "error inserting event")
	}

	if r.EventType == models.EventTypeMissedCall {
		rc := rt.RP.Get()
		defer rc.Close()

		err = handler.QueueTask(rc, r.OrgID, e.ContactID, &ctasks.ChannelEventTask{
			EventID:    e.ID,
			EventType:  e.EventType,
			ChannelID:  e.ChannelID,
			URNID:      e.URNID,
			Extra:      e.Extra,
			CreatedOn:  e.CreatedOn,
			NewContact: cu.newContact,
		})
		if err != nil {
			return nil, 0, errors.Wrap(err, "error queueing handle task")
		}
	}

	return map[string]any{"id": e.ID}, http.StatusOK, nil
}
