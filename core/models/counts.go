package models

import (
	"context"
	"time"

	"github.com/nyaruka/gocommon/dates"
)

type DailyCount struct {
	OrgID OrgID      `db:"org_id"`
	Day   dates.Date `db:"day"`
	Scope string     `db:"scope"`
	Count int64      `db:"count"`
}

const sqlInsertDailyCount = `INSERT INTO orgs_dailycount(org_id, scope, day, count, is_squashed) VALUES(:org_id, :scope, :day, :count, FALSE)`

// InsertDailyCounts inserts daily counts for the given org for today.
func InsertDailyCounts(ctx context.Context, tx DBorTx, oa *OrgAssets, when time.Time, scopeCounts map[string]int) error {
	day := dates.ExtractDate(when.In(oa.Env().Timezone()))
	counts := make([]*DailyCount, 0, len(scopeCounts))

	for scope, count := range scopeCounts {
		counts = append(counts, &DailyCount{OrgID: oa.OrgID(), Day: day, Scope: scope, Count: int64(count)})
	}

	return BulkQuery(ctx, "inserted daily counts", tx, sqlInsertDailyCount, counts)
}
