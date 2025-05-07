package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	models.RegisterEventHandler(events.TypeOptInRequested, handleOptInRequested)
}

func handleOptInRequested(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.OptInRequestedEvent)

	slog.Debug("optin requested", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), slog.Group("optin", "uuid", event.OptIn.UUID, "name", event.OptIn.Name))

	// get our opt in
	optIn := oa.OptInByUUID(event.OptIn.UUID)
	if optIn == nil {
		return fmt.Errorf("unable to load optin with uuid: %s", event.OptIn.UUID)
	}

	// get our channel
	channel := oa.ChannelByUUID(event.Channel.UUID)
	if channel == nil {
		return fmt.Errorf("unable to load channel with uuid: %s", event.Channel.UUID)
	}

	// and the flow
	flow, _ := scene.Session().LocateEvent(e)

	msg := models.NewOutgoingOptInMsg(rt, oa.OrgID(), scene.Session(), flow, optIn, channel, event.URN, event.CreatedOn())

	// register to have this message committed and sent
	scene.AddToPreCommitHook(hooks.CommitMessagesHook, hooks.MsgAndURN{Msg: msg, URN: event.URN})
	scene.AddToPostCommitHook(hooks.SendMessagesHook, msg)

	return nil
}
