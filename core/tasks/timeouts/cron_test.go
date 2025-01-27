package timeouts_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/core/tasks/timeouts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestTimeouts(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// create sessions for Bob and Cathy that have timed out and session for George that has not
	s1TimeoutOn := time.Now().Add(-time.Second)
	s1ID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), false, &s1TimeoutOn)
	s2TimeoutOn := time.Now().Add(-time.Second * 30)
	s2ID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), false, &s2TimeoutOn)
	s3TimeoutOn := time.Now().Add(time.Hour * 24)
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.George, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), false, &s3TimeoutOn)

	// for other org create 6 waiting sessions
	for i := range 6 {
		c := testdata.InsertContact(rt, testdata.Org2, flows.ContactUUID(uuids.NewV4()), fmt.Sprint(i), i18n.NilLanguage, models.ContactStatusActive)
		timeoutOn := time.Now().Add(-time.Second * 10)
		testdata.InsertWaitingSession(rt, testdata.Org2, c, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), false, &timeoutOn)
	}

	// schedule our timeouts
	cron := timeouts.NewTimeoutsCron(5)
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"dupes": 0, "queued": 8}, res)

	// should have created one throttled task for org 1
	task1, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org1.ID), task1.OwnerID)
	assert.Equal(t, "bulk_session_timeout", task1.Type)

	decoded := &contacts.BulkSessionTimeoutTask{}
	jsonx.MustUnmarshal(task1.Task, decoded)
	assert.Len(t, decoded.Timeouts, 2)
	assert.Equal(t, s2ID, decoded.Timeouts[0].SessionID)
	assert.Equal(t, s1ID, decoded.Timeouts[1].SessionID)

	// and two for org 2
	task2, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org2.ID), task2.OwnerID)
	assert.Equal(t, "bulk_session_timeout", task2.Type)
	task3, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org2.ID), task3.OwnerID)
	assert.Equal(t, "bulk_session_timeout", task2.Type)

	// no other
	task, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Nil(t, task)

	// if task runs again, these timeouts won't be re-queued
	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"dupes": 8, "queued": 0}, res)
}
