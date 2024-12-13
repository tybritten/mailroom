package mailroom

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/appleboy/go-fcm"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/aws/s3x"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/nyaruka/redisx"
)

// Mailroom is a service for handling RapidPro events
type Mailroom struct {
	ctx    context.Context
	cancel context.CancelFunc

	rt   *runtime.Runtime
	wg   *sync.WaitGroup
	quit chan bool

	handlerForeman   *Foreman
	batchForeman     *Foreman
	throttledForeman *Foreman

	webserver *web.Server
}

// NewMailroom creates and returns a new mailroom instance
func NewMailroom(config *runtime.Config) *Mailroom {
	mr := &Mailroom{
		rt:   &runtime.Runtime{Config: config},
		quit: make(chan bool),
		wg:   &sync.WaitGroup{},
	}
	mr.ctx, mr.cancel = context.WithCancel(context.Background())

	mr.handlerForeman = NewForeman(mr.rt, mr.wg, tasks.HandlerQueue, config.HandlerWorkers)
	mr.batchForeman = NewForeman(mr.rt, mr.wg, tasks.BatchQueue, config.BatchWorkers)
	mr.throttledForeman = NewForeman(mr.rt, mr.wg, tasks.ThrottledQueue, config.BatchWorkers)

	return mr
}

// Start starts the mailroom service
func (mr *Mailroom) Start() error {
	c := mr.rt.Config

	log := slog.With("comp", "mailroom")

	var err error
	_, mr.rt.DB, err = openAndCheckDBConnection(c.DB, c.DBPoolSize)
	if err != nil {
		log.Error("db not reachable", "error", err)
	} else {
		log.Info("db ok")
	}

	if c.ReadonlyDB != "" {
		mr.rt.ReadonlyDB, _, err = openAndCheckDBConnection(c.ReadonlyDB, c.DBPoolSize)
		if err != nil {
			log.Error("readonly db not reachable", "error", err)
		} else {
			log.Info("readonly db ok")
		}
	} else {
		// if readonly DB not specified, just use default DB again
		mr.rt.ReadonlyDB = mr.rt.DB.DB
		log.Warn("no distinct readonly db configured")
	}

	mr.rt.RP, err = redisx.NewPool(c.Redis)
	if err != nil {
		log.Error("redis not reachable", "error", err)
	} else {
		log.Info("redis ok")
	}

	if c.AndroidCredentialsFile != "" {
		mr.rt.FCM, err = fcm.NewClient(mr.ctx, fcm.WithCredentialsFile(c.AndroidCredentialsFile))
		if err != nil {
			log.Error("unable to create FCM client", "error", err)
		}
	} else {
		log.Warn("fcm not configured, no android syncing")
	}

	// setup DynamoDB
	mr.rt.Dynamo, err = dynamo.NewService(c.AWSAccessKeyID, c.AWSSecretAccessKey, c.AWSRegion, c.DynamoEndpoint, c.DynamoTablePrefix)
	if err != nil {
		return err
	}
	if err := mr.rt.Dynamo.Test(mr.ctx); err != nil {
		log.Error("dynamodb not reachable", "error", err)
	} else {
		log.Info("dynamodb ok")
	}

	// setup S3 storage
	mr.rt.S3, err = s3x.NewService(c.AWSAccessKeyID, c.AWSSecretAccessKey, c.AWSRegion, c.S3Endpoint, c.S3Minio)
	if err != nil {
		return err
	}

	// check buckets
	if err := mr.rt.S3.Test(mr.ctx, c.S3AttachmentsBucket); err != nil {
		log.Error("attachments bucket not accessible", "error", err)
	} else {
		log.Info("attachments bucket ok")
	}
	if err := mr.rt.S3.Test(mr.ctx, c.S3SessionsBucket); err != nil {
		log.Error("sessions bucket not accessible", "error", err)
	} else {
		log.Info("sessions bucket ok")
	}

	// initialize our elastic client
	mr.rt.ES, err = elasticsearch.NewTypedClient(elasticsearch.Config{Addresses: []string{c.Elastic}, Username: c.ElasticUsername, Password: c.ElasticPassword})
	if err != nil {
		log.Error("elastic search not available", "error", err)
	} else {
		log.Info("elastic ok")
	}

	// if we have a librato token, configure it
	if c.LibratoToken != "" {
		analytics.RegisterBackend(analytics.NewLibrato(c.LibratoUsername, c.LibratoToken, c.InstanceID, time.Second, mr.wg))
	}

	analytics.Start()

	// configure and start cloudwatch
	mr.rt.CW, err = cwatch.NewService(c.AWSAccessKeyID, c.AWSSecretAccessKey, c.AWSRegion, c.CloudwatchNamespace, c.DeploymentID)
	if err != nil {
		log.Error("cloudwatch not available", "error", err)
	} else {
		log.Info("cloudwatch ok")
	}

	mr.rt.CW.StartQueue(mr.wg, time.Second*3)

	// init our foremen and start it
	mr.handlerForeman.Start()
	mr.batchForeman.Start()
	mr.throttledForeman.Start()

	// start our web server
	mr.webserver = web.NewServer(mr.ctx, mr.rt, mr.wg)
	mr.webserver.Start()

	tasks.StartCrons(mr.rt, mr.wg, mr.quit)

	log.Info("mailroom started", "domain", c.Domain)

	return nil
}

// Stop stops the mailroom service
func (mr *Mailroom) Stop() error {
	log := slog.With("comp", "mailroom")
	log.Info("mailroom stopping")

	mr.handlerForeman.Stop()
	mr.batchForeman.Stop()
	mr.throttledForeman.Stop()

	mr.rt.CW.StopQueue()
	analytics.Stop()

	close(mr.quit)
	mr.cancel()

	// stop our web server
	mr.webserver.Stop()

	mr.wg.Wait()

	log.Info("mailroom stopped")
	return nil
}

func openAndCheckDBConnection(url string, maxOpenConns int) (*sql.DB, *sqlx.DB, error) {
	db, err := sqlx.Open("postgres", url)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to open database connection: '%s': %w", url, err)
	}

	// configure our pool
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetConnMaxLifetime(time.Minute * 30)

	// ping database...
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = db.PingContext(ctx)
	cancel()

	return db.DB, db, err
}
