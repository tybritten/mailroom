package handlers

import (
	"context"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	runner.RegisterEventHandler(events.TypeContactStatusChanged, handleContactStatusChanged)
}

// handleContactStatusChanged updates contact status
func handleContactStatusChanged(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event) error {
	event := e.(*events.ContactStatusChangedEvent)

	slog.Debug("contact status changed", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "status", event.Status)

	scene.AttachHook(hooks.UpdateContactStatus, event)
	scene.AttachHook(hooks.UpdateContactModifiedOn, event)

	return nil
}
