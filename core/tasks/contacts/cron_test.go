package contacts_test

import (
	"cmp"
	"slices"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/utils/queues"
	"github.com/stretchr/testify/assert"
)

func TestFiresCron(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	testdata.InsertContactFire(rt, testdata.Org1, testdata.Cathy, models.ContactFireTypeWaitExpiration, "", map[string]any{"session_id": 1234, "session_modified_on": "2025-01-22T17:55:00Z"}, time.Now().Add(-1*time.Second))
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Cathy, models.ContactFireTypeWaitTimeout, "", map[string]any{"session_id": 1234, "session_modified_on": "2025-01-22T17:55:00Z"}, time.Now().Add(3*time.Second))
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Bob, models.ContactFireTypeWaitExpiration, "", map[string]any{"session_id": 2345, "session_modified_on": "2025-01-22T17:56:00Z"}, time.Now().Add(-3*time.Second))
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Bob, models.ContactFireTypeWaitTimeout, "", map[string]any{"session_id": 1234, "session_modified_on": "2025-01-22T17:55:00Z"}, time.Now().Add(3*time.Second))
	testdata.InsertContactFire(rt, testdata.Org1, testdata.George, models.ContactFireTypeWaitExpiration, "", map[string]any{"session_id": 3456, "session_modified_on": "2025-01-22T17:57:00Z", "call_id": 23}, time.Now().Add(-time.Second))
	testdata.InsertContactFire(rt, testdata.Org1, testdata.George, models.ContactFireTypeWaitTimeout, "", map[string]any{"session_id": 3456, "session_modified_on": "2025-01-22T17:57:00Z"}, time.Now().Add(-time.Second))
	testdata.InsertContactFire(rt, testdata.Org2, testdata.Org2Contact, models.ContactFireTypeWaitTimeout, "", map[string]any{"session_id": 4567, "session_modified_on": "2025-01-22T17:58:00Z"}, time.Now().Add(-time.Second))

	cron := contacts.NewFiresCron(3, 5)
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"expires": 2, "hangups": 1, "timeouts": 2}, res)

	// should have created 3 throttled tasks.. unfortunately order is not guaranteed so we sort them
	var ts []*queues.Task
	for i := 0; i < 3; i++ {
		task, err := tasks.ThrottledQueue.Pop(rc)
		assert.NoError(t, err)
		ts = append(ts, task)
	}
	slices.SortFunc(ts, func(a, b *queues.Task) int {
		return cmp.Or(cmp.Compare(a.OwnerID, b.OwnerID), cmp.Compare(a.Type, b.Type))
	})

	assert.Equal(t, int(testdata.Org1.ID), ts[0].OwnerID)
	assert.Equal(t, "bulk_session_expire", ts[0].Type)
	assert.Equal(t, int(testdata.Org1.ID), ts[1].OwnerID)
	assert.Equal(t, "bulk_session_timeout", ts[1].Type)
	assert.Equal(t, int(testdata.Org2.ID), ts[2].OwnerID)
	assert.Equal(t, "bulk_session_timeout", ts[2].Type)

	decoded1 := &contacts.BulkSessionExpireTask{}
	jsonx.MustUnmarshal(ts[0].Task, decoded1)
	assert.Len(t, decoded1.Expirations, 2)
	assert.Equal(t, models.SessionID(2345), decoded1.Expirations[0].SessionID)
	assert.Equal(t, time.Date(2025, 1, 22, 17, 56, 0, 0, time.UTC), decoded1.Expirations[0].ModifiedOn)
	assert.Equal(t, models.SessionID(1234), decoded1.Expirations[1].SessionID)
	assert.Equal(t, time.Date(2025, 1, 22, 17, 55, 0, 0, time.UTC), decoded1.Expirations[1].ModifiedOn)

	decoded2 := &contacts.BulkSessionTimeoutTask{}
	jsonx.MustUnmarshal(ts[1].Task, decoded2)
	assert.Len(t, decoded2.Timeouts, 1)
	assert.Equal(t, models.SessionID(3456), decoded2.Timeouts[0].SessionID)

	// the hangup task should have ended up in the batch queue
	task, err := tasks.BatchQueue.Pop(rc)
	assert.NoError(t, err)
	assert.Equal(t, int(testdata.Org1.ID), task.OwnerID)
	assert.Equal(t, "bulk_call_hangup", task.Type)

	decoded3 := &ivr.BulkCallHangupTask{}
	jsonx.MustUnmarshal(task.Task, decoded3)
	assert.Len(t, decoded3.Hangups, 1)
	assert.Equal(t, models.SessionID(3456), decoded3.Hangups[0].SessionID)
	assert.Equal(t, models.CallID(23), decoded3.Hangups[0].CallID)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire`).Returns(2) // only 2 fires in the future left

	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"expires": 0, "hangups": 0, "timeouts": 0}, res)
}
