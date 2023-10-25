package msgs

import (
	"context"
	"log/slog"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	mailroom.RegisterCron("retry_errored_messages", time.Second*60, false, RetryErroredMessages)
}

func RetryErroredMessages(ctx context.Context, rt *runtime.Runtime) error {
	rc := rt.RP.Get()
	defer rc.Close()

	start := time.Now()

	msgs, err := models.GetMessagesForRetry(ctx, rt.DB)
	if err != nil {
		return errors.Wrap(err, "error fetching errored messages to retry")
	}
	if len(msgs) == 0 {
		return nil // nothing to retry
	}

	err = models.MarkMessagesQueued(ctx, rt.DB, msgs)
	if err != nil {
		return errors.Wrap(err, "error marking messages as queued")
	}

	msgio.QueueMessages(ctx, rt, rt.DB, nil, msgs)

	slog.Info("retried errored messages", "count", len(msgs), "elapsed", time.Since(start))

	return nil
}
