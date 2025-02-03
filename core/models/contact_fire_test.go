package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactFires(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertContactFire(rt, testdata.Org1, testdata.Cathy, models.ContactFireTypeWaitExpiration, "", map[string]any{"session_id": 1234}, time.Now().Add(-5*time.Second))
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Bob, models.ContactFireTypeWaitExpiration, "", map[string]any{"session_id": 2345}, time.Now().Add(-4*time.Second))
	testdata.InsertContactFire(rt, testdata.Org2, testdata.Org2Contact, models.ContactFireTypeWaitExpiration, "", map[string]any{"session_id": 3456}, time.Now().Add(-3*time.Second))
	testdata.InsertContactFire(rt, testdata.Org2, testdata.Org2Contact, models.ContactFireTypeWaitTimeout, "", map[string]any{"session_id": 3456}, time.Now().Add(-2*time.Second))
	testdata.InsertContactFire(rt, testdata.Org1, testdata.Bob, models.ContactFireTypeCampaign, "235", map[string]any{}, time.Now().Add(2*time.Second))

	fires, err := models.LoadDueContactfires(ctx, rt, 3)
	assert.NoError(t, err)
	assert.Len(t, fires, 3)
	assert.Equal(t, testdata.Cathy.ID, fires[0].ContactID)
	assert.Equal(t, models.SessionID(1234), fires[0].Extra.V.SessionID)

	err = models.DeleteContactFires(ctx, rt, []*models.ContactFire{fires[0], fires[1]})
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire`, 2)
}

func TestSessionContactFires(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testFlows := testdata.ImportFlows(rt, testdata.Org1, "testdata/session_test_flows.json")
	flow := testFlows[0]

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshFlows)
	require.NoError(t, err)

	modelContact1, _, _ := testdata.Bob.Load(rt, oa)
	modelContact2, _, _ := testdata.Cathy.Load(rt, oa)

	_, flowSession1, sprint1 := test.NewSessionBuilder().WithAssets(oa.SessionAssets()).WithFlow(flow.UUID).
		WithContact(testdata.Bob.UUID, flows.ContactID(testdata.Bob.ID), "Bob", "eng", "").MustBuild()
	_, flowSession2, sprint2 := test.NewSessionBuilder().WithAssets(oa.SessionAssets()).WithFlow(flow.UUID).
		WithContact(testdata.Cathy.UUID, flows.ContactID(testdata.Cathy.ID), "Cathy", "eng", "").MustBuild()

	tx := rt.DB.MustBegin()

	modelSessions, err := models.InsertSessions(ctx, rt, tx, oa, []flows.Session{flowSession1, flowSession2}, []flows.Sprint{sprint1, sprint2}, []*models.Contact{modelContact1, modelContact2}, nil, models.NilStartID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire WHERE contact_id = $1 AND fire_type = 'E'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire WHERE contact_id = $1 AND fire_type = 'T'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire WHERE contact_id = $1 AND fire_type = 'E'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire WHERE contact_id = $1 AND fire_type = 'T'`, testdata.Cathy.ID).Returns(1)

	// check session modified on is stored as UTC
	assertdb.Query(t, rt.DB, `SELECT extra->>'session_modified_on' FROM contacts_contactfire WHERE contact_id = $1 AND fire_type = 'E'`, testdata.Bob.ID).
		Returns(modelSessions[0].ModifiedOn().In(time.UTC).Format(time.RFC3339Nano))

	num, err := models.DeleteSessionContactFires(ctx, rt.DB, []models.ContactID{testdata.Bob.ID})
	assert.NoError(t, err)
	assert.Equal(t, 2, num)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire WHERE contact_id = $1`, testdata.Bob.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire WHERE contact_id = $1`, testdata.Cathy.ID).Returns(2)
}
