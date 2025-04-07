package handlers

import (
	"context"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	models.RegisterEventHandler(events.TypeContactRefreshed, noopHandler)
	models.RegisterEventHandler(events.TypeEnvironmentRefreshed, noopHandler)
	models.RegisterEventHandler(events.TypeError, noopHandler)
	models.RegisterEventHandler(events.TypeFailure, noopHandler)
	models.RegisterEventHandler(events.TypeLLMCalled, noopHandler)
	models.RegisterEventHandler(events.TypeMsgWait, noopHandler)
	models.RegisterEventHandler(events.TypeRunExpired, noopHandler)
	models.RegisterEventHandler(events.TypeRunResultChanged, noopHandler)
	models.RegisterEventHandler(events.TypeServiceCalled, noopHandler)
	models.RegisterEventHandler(events.TypeWaitTimedOut, noopHandler)
	models.RegisterEventHandler(events.TypeDialWait, noopHandler)
	models.RegisterEventHandler(events.TypeDialEnded, noopHandler)
}

// our hook for events we ignore in a run
func noopHandler(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, event flows.Event) error {
	slog.Debug("noop event", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "type", event.Type())

	return nil
}
