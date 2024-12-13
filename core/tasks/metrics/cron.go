package analytics

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	tasks.RegisterCron("metrics", &metricsCron{})
}

// calculates a bunch of stats every minute and both logs them and sends them to cloudwatch
type metricsCron struct {
	// both sqlx and redis provide wait stats which are cummulative that we need to make into increments
	dbWaitDuration    time.Duration
	redisWaitDuration time.Duration
}

func (c *metricsCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

func (c *metricsCron) AllInstances() bool {
	return true
}

func (c *metricsCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	// TODO replace with offset passed to tasks.CronNext
	// We wait 15 seconds since we fire at the top of the minute, the same as expirations.
	// That way any metrics related to the size of our queue are a bit more accurate (all expirations can
	// usually be handled in 15 seconds). Something more complicated would take into account the age of
	// the items in our queues.
	time.Sleep(time.Second * 15)

	handlerSize, batchSize, throttledSize := getQueueSizes(rt)

	// get our DB and redis stats
	dbStats := rt.DB.Stats()
	redisStats := rt.RP.Stats()

	dbWaitDurationInPeriod := dbStats.WaitDuration - c.dbWaitDuration
	redisWaitDurationInPeriod := redisStats.WaitDuration - c.redisWaitDuration

	c.dbWaitDuration = dbStats.WaitDuration
	c.redisWaitDuration = redisStats.WaitDuration

	dims := []types.Dimension{
		{Name: aws.String("Host"), Value: aws.String(rt.Config.InstanceID)},
		{Name: aws.String("App"), Value: aws.String("mailroom")},
	}

	rt.CW.Queue(types.MetricDatum{
		MetricName: aws.String("DBConnectionsInUse"),
		Dimensions: dims,
		Value:      aws.Float64(float64(dbStats.InUse)),
		Unit:       types.StandardUnitCount,
	}, types.MetricDatum{
		MetricName: aws.String("DBConnectionWaitDuration"),
		Dimensions: dims,
		Value:      aws.Float64(float64(dbWaitDurationInPeriod / time.Second)),
		Unit:       types.StandardUnitSeconds,
	}, types.MetricDatum{
		MetricName: aws.String("RedisConnectionsInUse"),
		Dimensions: dims,
		Value:      aws.Float64(float64(redisStats.ActiveCount)),
		Unit:       types.StandardUnitCount,
	}, types.MetricDatum{
		MetricName: aws.String("RedisConnectionsWaitDuration"),
		Dimensions: dims,
		Value:      aws.Float64(float64(redisWaitDurationInPeriod / time.Second)),
		Unit:       types.StandardUnitSeconds,
	})

	rt.CW.Queue(types.MetricDatum{
		MetricName: aws.String("QueuedTasks"),
		Dimensions: []types.Dimension{
			{Name: aws.String("QueueName"), Value: aws.String("handler")},
		},
		Value: aws.Float64(float64(handlerSize)),
		Unit:  types.StandardUnitCount,
	}, types.MetricDatum{
		MetricName: aws.String("QueuedTasks"),
		Dimensions: []types.Dimension{
			{Name: aws.String("QueueName"), Value: aws.String("batch")},
		},
		Value: aws.Float64(float64(batchSize)),
		Unit:  types.StandardUnitCount,
	}, types.MetricDatum{
		MetricName: aws.String("QueuedTasks"),
		Dimensions: []types.Dimension{
			{Name: aws.String("QueueName"), Value: aws.String("throttled")},
		},
		Value: aws.Float64(float64(throttledSize)),
		Unit:  types.StandardUnitCount,
	})

	return map[string]any{
		"db_inuse":       dbStats.InUse,
		"db_wait":        dbWaitDurationInPeriod,
		"redis_inuse":    redisStats.ActiveCount,
		"redis_wait":     redisWaitDurationInPeriod,
		"handler_size":   handlerSize,
		"batch_size":     batchSize,
		"throttled_size": throttledSize,
	}, nil
}

func getQueueSizes(rt *runtime.Runtime) (int, int, int) {
	rc := rt.RP.Get()
	defer rc.Close()

	handler, err := tasks.HandlerQueue.Size(rc)
	if err != nil {
		slog.Error("error calculating handler queue size", "error", err)
	}
	batch, err := tasks.BatchQueue.Size(rc)
	if err != nil {
		slog.Error("error calculating batch queue size", "error", err)
	}
	throttled, err := tasks.ThrottledQueue.Size(rc)
	if err != nil {
		slog.Error("error calculating throttled queue size", "error", err)
	}

	return handler, batch, throttled
}
