package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	models.RegisterEventHandler(events.TypeIVRCreated, handleIVRCreated)
}

// handleIVRCreated creates the db msg for the passed in event
func handleIVRCreated(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.IVRCreatedEvent)

	slog.Debug("ivr created", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "text", event.Msg.Text())

	// get our call
	call := scene.Session().Call()
	if call == nil {
		return fmt.Errorf("ivr session must have a call set")
	}

	// if our call is no longer in progress, return
	if call.Status() != models.CallStatusWired && call.Status() != models.CallStatusInProgress {
		return nil
	}

	msg := models.NewOutgoingIVR(rt.Config, oa.OrgID(), call, event.Msg, event.CreatedOn())

	// register to have this message committed
	scene.AttachPreCommitHook(hooks.InsertIVRMessages, msg)

	return nil
}
