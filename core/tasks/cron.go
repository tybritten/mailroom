package tasks

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/runtime"
)

type Cron interface {
	Run(context.Context, *runtime.Runtime) (map[string]any, error)
}

// RegisterCron registers a new cron job
func RegisterCron(name string, interval time.Duration, allInstances bool, c Cron) {
	mailroom.RegisterCron(name, interval, allInstances, c.Run)
}
