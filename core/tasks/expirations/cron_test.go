package expirations_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/core/tasks/expirations"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestExpirations(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// create completed session for Cathy
	testdata.InsertFlowSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, testdata.Favorites, models.NilCallID)

	// create waiting session for George
	s2ID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.George, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), true, nil)

	// create waiting session for Bob with expiration in future
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now().Add(time.Hour), true, nil)

	// create an IVR session for Alexandria
	call := testdata.InsertCall(rt, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria)
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Alexandria, models.FlowTypeVoice, testdata.IVRFlow, call, time.Now(), time.Now(), false, nil)

	// for other org create 6 waiting sessions that will expire
	for i := range 6 {
		c := testdata.InsertContact(rt, testdata.Org2, flows.ContactUUID(uuids.NewV4()), fmt.Sprint(i), i18n.NilLanguage, models.ContactStatusActive)
		testdata.InsertWaitingSession(rt, testdata.Org2, c, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), true, nil)
	}

	// expire our sessions...
	cron := expirations.NewExpirationsCron(5)
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"dupes": 0, "queued": 7}, res)

	// should have created one throttled task for org 1
	task1, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org1.ID), task1.OwnerID)
	assert.Equal(t, "bulk_session_expire", task1.Type)

	decoded := &contacts.BulkSessionExpireTask{}
	jsonx.MustUnmarshal(task1.Task, decoded)
	assert.Len(t, decoded.Expirations, 1)
	assert.Equal(t, s2ID, decoded.Expirations[0].SessionID)

	// and two for org 2
	task2, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org2.ID), task2.OwnerID)
	assert.Equal(t, "bulk_session_expire", task2.Type)
	task3, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org2.ID), task3.OwnerID)
	assert.Equal(t, "bulk_session_expire", task2.Type)

	// no other
	task, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Nil(t, task)

	// if task runs again, these tasks won't be re-queued
	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"dupes": 7, "queued": 0}, res)
}

func TestExpireVoiceSessions(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// create voice session for Cathy
	conn1ID := testdata.InsertCall(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	s1ID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeVoice, testdata.IVRFlow, conn1ID, time.Now(), time.Now(), false, nil)
	r1ID := testdata.InsertFlowRun(rt, testdata.Org1, s1ID, testdata.Cathy, testdata.Favorites, models.RunStatusWaiting, "")

	// create voice session for Bob with expiration in future
	conn2ID := testdata.InsertCall(rt, testdata.Org1, testdata.TwilioChannel, testdata.Bob)
	s2ID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.IVRFlow, conn2ID, time.Now(), time.Now().Add(time.Hour), false, nil)
	r2ID := testdata.InsertFlowRun(rt, testdata.Org1, s2ID, testdata.Bob, testdata.IVRFlow, models.RunStatusWaiting, "")

	// create a messaging session for Alexandria
	s3ID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Alexandria, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), false, nil)
	r3ID := testdata.InsertFlowRun(rt, testdata.Org1, s3ID, testdata.Alexandria, testdata.Favorites, models.RunStatusWaiting, "")

	time.Sleep(5 * time.Millisecond)

	// expire our sessions...
	cron := &expirations.VoiceExpirationsCron{}
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"expired": 1}, res)

	// Cathy's session should be expired along with its runs
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1;`, s1ID).Columns(map[string]any{"status": "X"})
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowrun WHERE id = $1;`, r1ID).Columns(map[string]any{"status": "X"})

	// Bob's session and run should be unchanged
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1;`, s2ID).Columns(map[string]any{"status": "W"})
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowrun WHERE id = $1;`, r2ID).Columns(map[string]any{"status": "W"})

	// Alexandria's session and run should be unchanged because message expirations are handled separately
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowsession WHERE id = $1;`, s3ID).Columns(map[string]any{"status": "W"})
	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowrun WHERE id = $1;`, r3ID).Columns(map[string]any{"status": "W"})
}
