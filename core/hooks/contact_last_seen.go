package hooks

import (
	"context"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ContactLastSeenHook is our hook for contact changes that require an update to last_seen_on
var ContactLastSeenHook models.EventCommitHook = &contactLastSeenHook{}

type contactLastSeenHook struct{}

// Apply squashes and updates modified_on on all the contacts passed in
func (h *contactLastSeenHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]any) error {

	for scene := range scenes {

		err := models.UpdateContactLastSeenOn(ctx, tx, scene.ContactID(), dates.Now())
		if err != nil {
			return errors.Wrapf(err, "error updating last_seen_on on contacts")
		}
	}

	return nil
}
