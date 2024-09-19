package msgs

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

const TypeSendBroadcastBatch = "send_broadcast_batch"

func init() {
	tasks.RegisterType(TypeSendBroadcastBatch, func() tasks.Task { return &SendBroadcastBatchTask{} })
}

// SendBroadcastTask is the task send broadcast batches
type SendBroadcastBatchTask struct {
	*models.BroadcastBatch
}

func (t *SendBroadcastBatchTask) Type() string {
	return TypeSendBroadcastBatch
}

// Timeout is the maximum amount of time the task can run for
func (t *SendBroadcastBatchTask) Timeout() time.Duration {
	return time.Minute * 60
}

func (t *SendBroadcastBatchTask) WithAssets() models.Refresh {
	return models.RefreshNone
}

func (t *SendBroadcastBatchTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	// create this batch of messages
	msgs, err := t.BroadcastBatch.CreateMessages(ctx, rt, oa)
	if err != nil {
		return fmt.Errorf("error creating broadcast messages: %w", err)
	}

	msgio.QueueMessages(ctx, rt, rt.DB, msgs)

	// if this is our last batch, mark broadcast as done
	if t.IsLast {
		if err := (&models.Broadcast{ID: t.BroadcastBatch.BroadcastID}).SetComplete(ctx, rt.DB); err != nil {
			return fmt.Errorf("error marking broadcast as complete: %w", err)
		}
	}

	return nil
}
