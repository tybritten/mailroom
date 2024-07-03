package channels

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	tasks.RegisterCron("sync_android_channels", &SyncAndroidChannelsCron{})
}

type SyncAndroidChannelsCron struct {
	FCMClient msgio.FCMClient
}

func (s *SyncAndroidChannelsCron) AllInstances() bool {
	return true
}

func (s *SyncAndroidChannelsCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute*10)

}

func (s *SyncAndroidChannelsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {

	if s.FCMClient == nil {
		s.FCMClient = msgio.CreateFCMClient(ctx, rt.Config)
	}

	oldSeenAndroidChannels, err := getOldSeenAndroidChannels(ctx, rt.DB)
	if err != nil {
		return nil, fmt.Errorf("error loading old seen android channels: %w", err)
	}

	erroredCount := 0
	syncedCount := 0

	for _, channel := range oldSeenAndroidChannels {
		err := msgio.SyncAndroidChannel(ctx, s.FCMClient, &channel)
		if err != nil {
			slog.Error("error syncing messages", "error", err, "channel_uuid", channel.UUID())
			erroredCount += 1
		} else {
			syncedCount += 1
		}

	}

	return map[string]any{"synced": syncedCount, "errored": erroredCount}, nil

}

func getOldSeenAndroidChannels(ctx context.Context, db models.DBorTx) ([]models.Channel, error) {
	now := dates.Now()
	start := now.AddDate(0, 0, -7)
	end := now.Add(-time.Minute * 15)

	rows, err := db.QueryContext(ctx, sqlSelectOldSeenAndroidChannels)
	if err != nil {
		return nil, fmt.Errorf("error querying old seen android channels: from %s to %s: %w", start.String(), end.String(), err)
	}

	return models.ScanJSONRows(rows, func() models.Channel { return models.Channel{} })
}

const sqlSelectOldSeenAndroidChannels = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	c.id as id,
	c.uuid as uuid,
	c.org_id as org_id,
	c.name as name,
	c.channel_type as channel_type,
	COALESCE(c.tps, 10) as tps,
	c.address as address,
	c.config as config
FROM 
	channels_channel c
WHERE
	c.channel_type = 'A' AND 
	c.last_seen >= NOW() - INTERVAL '7 days' AND 
	c.last_seen <  NOW() - INTERVAL '15 minutes' AND
	c.is_active = TRUE
ORDER BY
	c.last_seen DESC, c.id DESC
) r;`
