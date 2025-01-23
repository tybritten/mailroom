package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
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
