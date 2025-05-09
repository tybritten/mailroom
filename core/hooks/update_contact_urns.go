package hooks

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// UpdateContactURNs is our hook for when a URN is added to a contact
var UpdateContactURNs models.SceneCommitHook = &updateContactURNs{}

type updateContactURNs struct{}

func (h *updateContactURNs) Order() int { return 1 }

func (h *updateContactURNs) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]any) error {
	var flowUUID assets.FlowUUID

	// gather all our urn changes, we only care about the last change for each scene
	changes := make([]*models.ContactURNsChanged, 0, len(scenes))
	for _, sessionChanges := range scenes {
		urnChange := sessionChanges[len(sessionChanges)-1].(*models.ContactURNsChanged)
		changes = append(changes, urnChange)

		if urnChange.Flow != nil {
			flowUUID = urnChange.Flow.UUID()
		}
	}

	affected, err := models.UpdateContactURNs(ctx, tx, oa, changes)
	if err != nil {
		return fmt.Errorf("error updating contact urns: %w", err)
	}

	if len(affected) > 0 {
		slog.Error("URN changes affected other contacts", "count", len(affected), "org_id", oa.OrgID(), "flow_uuid", flowUUID)
	}

	return nil
}
