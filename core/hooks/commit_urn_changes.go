package hooks

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
)

// CommitURNChangesHook is our hook for when a URN is added to a contact
var CommitURNChangesHook models.EventCommitHook = &commitURNChangesHook{}

type commitURNChangesHook struct{}

// Apply adds all our URNS in a batch
func (h *commitURNChangesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]any) error {
	// gather all our urn changes, we only care about the last change for each scene
	changes := make([]*models.ContactURNsChanged, 0, len(scenes))
	for _, sessionChanges := range scenes {
		changes = append(changes, sessionChanges[len(sessionChanges)-1].(*models.ContactURNsChanged))
	}

	affected, err := models.UpdateContactURNs(ctx, tx, oa, changes)
	if err != nil {
		return fmt.Errorf("error updating contact urns: %w", err)
	}

	if len(affected) > 0 {
		slog.Error("URN changes affected other contacts", "count", len(affected), "org_id", oa.OrgID())
	}

	return nil
}
