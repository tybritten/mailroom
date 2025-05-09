package hooks

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// UpdateCallStatus is our hook for updating IVR call status
var UpdateCallStatus models.SceneCommitHook = &updateCallStatus{}

type updateCallStatus struct{}

func (h *updateCallStatus) Order() int { return 1 }

func (h *updateCallStatus) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]any) error {
	for scene, es := range scenes {
		status := es[len(es)-1].(models.CallStatus)

		if err := scene.Call().UpdateStatus(ctx, tx, status, 0, time.Now()); err != nil {
			return fmt.Errorf("error updating call status: %w", err)
		}
	}

	return nil
}
