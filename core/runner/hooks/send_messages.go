package hooks

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
)

// SendMessages is our hook for sending scene messages
var SendMessages runner.SceneHook = &sendMessages{}

type sendMessages struct{}

func (h *sendMessages) Order() int { return 1 }

func (h *sendMessages) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*runner.Scene][]any) error {
	msgs := make([]*models.Msg, 0, 1)

	// for each scene gather all our messages
	for _, args := range scenes {
		sceneMsgs := make([]*models.Msg, 0, 1)

		for _, m := range args {
			sceneMsgs = append(sceneMsgs, m.(*models.Msg))
		}

		// mark the last message in the sprint (used for setting timeouts)
		sceneMsgs[len(sceneMsgs)-1].LastInSprint = true

		msgs = append(msgs, sceneMsgs...)
	}

	msgio.QueueMessages(ctx, rt, tx, msgs)
	return nil
}
