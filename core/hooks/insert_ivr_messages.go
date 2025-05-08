package hooks

import (
	"context"
	"fmt"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
)

// InsertIVRMessages is our hook for comitting scene messages / say commands
var InsertIVRMessages models.SceneCommitHook = &insertIVRMessages{}

type insertIVRMessages struct{}

func (h *insertIVRMessages) Order() int { return 1 }

func (h *insertIVRMessages) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]any) error {
	msgs := make([]*models.Msg, 0, len(scenes))
	for _, s := range scenes {
		for _, m := range s {
			msgs = append(msgs, m.(*models.Msg))
		}
	}

	if err := models.InsertMessages(ctx, tx, msgs); err != nil {
		return fmt.Errorf("error writing messages: %w", err)
	}

	return nil
}
