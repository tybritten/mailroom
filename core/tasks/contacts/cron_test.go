package contacts_test

import (
	"cmp"
	"slices"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/campaigns"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
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

	testdata.InsertContactFire(rt, testdata.Org1, testdata.Cathy, models.ContactFireTypeWaitExpiration, "", time.Now().Add(-1*time.Second), "f72b48df-5f6d-4e4f-955a-f5fb29ccb97b")
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Cathy, models.ContactFireTypeWaitTimeout, "", time.Now().Add(3*time.Second), "f72b48df-5f6d-4e4f-955a-f5fb29ccb97b")
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Bob, models.ContactFireTypeWaitExpiration, "", time.Now().Add(-3*time.Second), "4010a3b2-d1f2-42ae-9051-47d41a3ef923")
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Bob, models.ContactFireTypeWaitTimeout, "", time.Now().Add(3*time.Second), "4010a3b2-d1f2-42ae-9051-47d41a3ef923")
	testdata.InsertContactFire(rt, testdata.Org1, testdata.George, models.ContactFireTypeWaitTimeout, "", time.Now().Add(-time.Second), "5c1248e3-f669-4a72-83f4-a29292fdad4d")
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Alexandria, models.ContactFireTypeCampaign, "6789", time.Now().Add(-time.Second), "")
	testdata.InsertContactFire(rt, testdata.Org2, testdata.Org2Contact, models.ContactFireTypeWaitTimeout, "", time.Now().Add(-time.Second), "8edf3b3c-0081-4d31-b199-1502b3190eb7")

	cron := contacts.NewFiresCron(3, 5)
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"wait_expires": 2, "wait_timeouts": 2, "campaigns": 1}, res)

	// should have created 4 throttled tasks.. unfortunately order is not guaranteed so we sort them
	var ts []*queues.Task
	for i := 0; i < 4; i++ {
		task, err := tasks.ThrottledQueue.Pop(rc)
		assert.NoError(t, err)
		ts = append(ts, task)
	}
	slices.SortFunc(ts, func(a, b *queues.Task) int {
		return cmp.Or(cmp.Compare(a.OwnerID, b.OwnerID), cmp.Compare(a.Type, b.Type))
	})

	assert.Equal(t, int(testdata.Org1.ID), ts[0].OwnerID)
	assert.Equal(t, "bulk_campaign_trigger", ts[0].Type)
	assert.Equal(t, int(testdata.Org1.ID), ts[1].OwnerID)
	assert.Equal(t, "bulk_wait_expire", ts[1].Type)
	assert.Equal(t, int(testdata.Org1.ID), ts[2].OwnerID)
	assert.Equal(t, "bulk_wait_timeout", ts[2].Type)
	assert.Equal(t, int(testdata.Org2.ID), ts[3].OwnerID)
	assert.Equal(t, "bulk_wait_timeout", ts[3].Type)

	decoded1 := &campaigns.BulkCampaignTriggerTask{}
	jsonx.MustUnmarshal(ts[0].Task, decoded1)
	assert.Len(t, decoded1.ContactIDs, 1)
	assert.Equal(t, testdata.Alexandria.ID, decoded1.ContactIDs[0])
	assert.Equal(t, models.CampaignEventID(6789), decoded1.EventID)

	decoded2 := &contacts.BulkWaitExpireTask{}
	jsonx.MustUnmarshal(ts[1].Task, decoded2)
	assert.Len(t, decoded2.Expirations, 2)
	assert.Equal(t, flows.SessionUUID("4010a3b2-d1f2-42ae-9051-47d41a3ef923"), decoded2.Expirations[0].SessionUUID)
	assert.Equal(t, flows.SessionUUID("f72b48df-5f6d-4e4f-955a-f5fb29ccb97b"), decoded2.Expirations[1].SessionUUID)

	decoded3 := &contacts.BulkWaitTimeoutTask{}
	jsonx.MustUnmarshal(ts[2].Task, decoded3)
	assert.Len(t, decoded3.Timeouts, 1)
	assert.Equal(t, flows.SessionUUID("5c1248e3-f669-4a72-83f4-a29292fdad4d"), decoded3.Timeouts[0].SessionUUID)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire`).Returns(2) // only 2 fires in the future left

	res, err = cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"wait_expires": 0, "wait_timeouts": 0, "campaigns": 0}, res)
}
