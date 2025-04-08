package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	models.RegisterEventHandler(events.TypeLLMCalled, handleLLMCalled)
}

func handleLLMCalled(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.LLMCalledEvent)

	slog.Debug("LLM called", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), slog.Group("llm", "uuid", event.LLM.UUID, "name", event.LLM.Name), "elapsed_ms", event.ElapsedMS)

	llm := oa.SessionAssets().LLMs().Get(event.LLM.UUID)
	if llm != nil {
		rt.Stats.RecordLLMCall(llm.Type(), time.Duration(event.ElapsedMS)*time.Millisecond)
	}

	return nil
}
