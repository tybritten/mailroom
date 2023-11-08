package testdata

import (
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func InsertSchedule(rt *runtime.Runtime, org *Org, repeat models.RepeatPeriod, next time.Time) models.ScheduleID {
	var id models.ScheduleID
	must(rt.DB.Get(&id,
		`INSERT INTO schedules_schedule(org_id, repeat_period, repeat_hour_of_day, repeat_minute_of_hour, next_fire)
		VALUES($1, $2, 12, 0, $3) RETURNING id`, org.ID, repeat, next,
	))

	return id
}
