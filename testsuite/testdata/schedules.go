package testdata

import (
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func InsertSchedule(rt *runtime.Runtime, org *Org, repeat models.RepeatPeriod, next time.Time) models.ScheduleID {
	var id models.ScheduleID
	must(rt.DB.Get(&id,
		`INSERT INTO schedules_schedule(org_id, repeat_period, next_fire, is_active, created_on, modified_on, created_by_id, modified_by_id)
		VALUES($1, $2, $3, TRUE, NOW(), NOW(), 1, 1) RETURNING id`, org.ID, repeat, next,
	))

	return id
}
