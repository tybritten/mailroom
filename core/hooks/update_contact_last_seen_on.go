package hooks

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// UpdateContactLastSeenOn is our hook for contact changes that require an update to last_seen_on
var UpdateContactLastSeenOn models.SceneCommitHook = &updateContactLastSeenOn{}

type updateContactLastSeenOn struct{}

func (h *updateContactLastSeenOn) Order() int { return 1 }

func (h *updateContactLastSeenOn) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]any) error {
	for scene, evts := range scenes {
		lastEvent := evts[len(evts)-1].(flows.Event)
		lastSeenOn := lastEvent.CreatedOn()

		if err := models.UpdateContactLastSeenOn(ctx, tx, scene.ContactID(), lastSeenOn); err != nil {
			return fmt.Errorf("error updating last_seen_on on contacts: %w", err)
		}
	}

	return nil
}
