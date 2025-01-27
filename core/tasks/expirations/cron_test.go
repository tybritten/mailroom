package expirations_test

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
	"github.com/nyaruka/mailroom/core/tasks/expirations"
	"github.com/nyaruka/mailroom/core/tasks/ivr"
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
	s2ID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.George, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), true, nil)

	// create waiting session for Bob with expiration in future
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now().Add(time.Hour), true, nil)

	// create an IVR session for Alexandria
	call := testdata.InsertCall(rt, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria)
	ivrID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Alexandria, models.FlowTypeVoice, testdata.IVRFlow, call, time.Now(), false, nil)

	// for other org create 6 waiting sessions that will expire
	for i := range 6 {
		c := testdata.InsertContact(rt, testdata.Org2, flows.ContactUUID(uuids.NewV4()), fmt.Sprint(i), i18n.NilLanguage, models.ContactStatusActive)
		testdata.InsertWaitingSession(rt, testdata.Org2, c, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), true, nil)
	}

	// expire our sessions...
	cron := expirations.NewExpirationsCron(5)
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"dupes": 0, "queued_expires": 7, "queued_hangups": 1}, res)

	// should have created one throttled expire task for org 1
	task1, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org1.ID), task1.OwnerID)
	assert.Equal(t, "bulk_session_expire", task1.Type)

	decoded1 := &contacts.BulkSessionExpireTask{}
	jsonx.MustUnmarshal(task1.Task, decoded1)
	assert.Len(t, decoded1.Expirations, 1)
	assert.Equal(t, s2ID, decoded1.Expirations[0].SessionID)

	// and one batch hangup task for the IVR session
	task2, err := tasks.BatchQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org1.ID), task2.OwnerID)
	assert.Equal(t, "bulk_call_hangup", task2.Type)

	decoded2 := &ivr.BulkCallHangupTask{}
	jsonx.MustUnmarshal(task2.Task, decoded2)
	assert.Len(t, decoded2.Hangups, 1)
	assert.Equal(t, ivrID, decoded2.Hangups[0].SessionID)

	// and two expire tasks for org 2
	task3, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org2.ID), task3.OwnerID)
	assert.Equal(t, "bulk_session_expire", task3.Type)
	task4, err := tasks.ThrottledQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org2.ID), task4.OwnerID)
	assert.Equal(t, "bulk_session_expire", task4.Type)

	// no other
	assert.Equal(t, map[string]int{}, testsuite.FlushTasks(t, rt, "batch", "throttled"))

	// if task runs again, these tasks won't be re-queued
	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"dupes": 8, "queued_expires": 0, "queued_hangups": 0}, res)
}
