package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	runner.RegisterEventHandler(events.TypeInputLabelsAdded, handleInputLabelsAdded)
}

// handleInputLabelsAdded is called for each input labels added event in a scene
func handleInputLabelsAdded(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, e flows.Event) error {
	event := e.(*events.InputLabelsAddedEvent)

	slog.Debug("input labels added", "contact", scene.ContactUUID(), "session", scene.SessionUUID(), "labels", event.Labels)

	inputMsgID := scene.IncomingMsgID()
	if inputMsgID != models.NilMsgID {
		for _, l := range event.Labels {
			label := oa.LabelByUUID(l.UUID)
			if label == nil {
				return fmt.Errorf("unable to find label with UUID: %s", l.UUID)
			}

			scene.AttachHook(hooks.AddMessageLabels, &models.MsgLabelAdd{MsgID: inputMsgID, LabelID: label.ID()})
		}
	}

	return nil
}
