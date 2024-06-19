package msg

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/queues"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/msg/broadcast", web.RequireAuthToken(web.JSONPayload(handleBroadcast)))
}

// Request to send a broadcast.
//
//	{
//	  "org_id": 1,
//	  "user_id": 56,
//	  "translations": {"eng": {"text": "Hello @contact"}, "spa": {"text": "Hola @contact"}},
//	  "base_language": "eng",
//	  "group_ids": [101, 102],
//	  "contact_ids": [4646],
//	  "urns": [4646],
//	  "optin_id": 456
//	}
type broadcastRequest struct {
	OrgID        models.OrgID                `json:"org_id"        validate:"required"`
	UserID       models.UserID               `json:"user_id"       validate:"required"`
	Translations flows.BroadcastTranslations `json:"translations"  validate:"required"`
	BaseLanguage i18n.Language               `json:"base_language" validate:"required"`
	ContactIDs   []models.ContactID          `json:"contact_ids"`
	GroupIDs     []models.GroupID            `json:"group_ids"`
	URNs         []urns.URN                  `json:"urns"`
	Query        string                      `json:"query"`
	NodeUUID     flows.NodeUUID              `json:"node_uuid"`
	OptInID      models.OptInID              `json:"optin_id"`
}

// handles a request to create the given broadcast
func handleBroadcast(ctx context.Context, rt *runtime.Runtime, r *broadcastRequest) (any, int, error) {
	// if a node is specified, get all the contacts at that node
	if r.NodeUUID != "" {
		contactIDs, err := models.GetContactIDsAtNode(ctx, rt, r.OrgID, r.NodeUUID)
		if err != nil {
			return nil, 0, fmt.Errorf("error getting contacts at node %s: %w", r.NodeUUID, err)
		}

		r.ContactIDs = append(r.ContactIDs, contactIDs...)
	}

	bcast := models.NewBroadcast(r.OrgID, r.Translations, models.TemplateStateUnevaluated, r.BaseLanguage, r.OptInID, r.URNs, r.ContactIDs, r.GroupIDs, r.Query, r.UserID)

	tx, err := rt.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("error beginning transaction: %w", err)
	}

	if err := models.InsertBroadcast(ctx, tx, bcast); err != nil {
		return nil, 0, fmt.Errorf("error inserting broadcast: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("error committing transaction: %w", err)
	}

	task := &msgs.SendBroadcastTask{Broadcast: bcast}

	rc := rt.RP.Get()
	defer rc.Close()
	err = tasks.Queue(rc, tasks.BatchQueue, bcast.OrgID, task, queues.HighPriority)
	if err != nil {
		slog.Error("error queueing broadcast task", "error", err)
	}

	return map[string]any{"id": bcast.ID}, http.StatusOK, nil
}
