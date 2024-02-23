package handlers

import (
	"context"
	"log/slog"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	models.RegisterEventHandler(events.TypeWarning, handleWarning)
}

func handleWarning(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.WarningEvent)

	run, _ := scene.Session().FindStep(e.StepUUID())
	flow, _ := oa.FlowByUUID(run.FlowReference().UUID)
	if flow != nil && strings.Contains(event.Text, "webhook recreated from extra") {
		// so that we can track these in sentry
		slog.Error("webhook recreated from extra usage", "session", scene.SessionID(), "flow", flow.UUID(), "text", event.Text)
	}

	return nil
}
