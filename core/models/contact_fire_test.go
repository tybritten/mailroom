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

	rt.DB.MustExec(`INSERT INTO contacts_contactfire(org_id, contact_id, fire_type, scope, extra, fire_on) VALUES (1, $1, 'E', '', '{"session_id": 1234}', '2025-01-17T11:58:00Z')`, testdata.Cathy.ID)
	rt.DB.MustExec(`INSERT INTO contacts_contactfire(org_id, contact_id, fire_type, scope, extra, fire_on) VALUES (1, $1, 'E', '', '{"session_id": 2345}', '2025-01-17T11:58:30Z')`, testdata.Bob.ID)
	rt.DB.MustExec(`INSERT INTO contacts_contactfire(org_id, contact_id, fire_type, scope, extra, fire_on) VALUES (2, $1, 'E', '', '{"session_id": 3456}', '2025-01-17T11:59:00Z')`, testdata.Org2Contact.ID)
	rt.DB.MustExec(`INSERT INTO contacts_contactfire(org_id, contact_id, fire_type, scope, extra, fire_on) VALUES (1, $1, 'C', '4567', '{}', $2)`, testdata.Bob.ID, time.Now().Add(time.Hour))

	fires, err := models.LoadDueContactfires(ctx, rt)
	assert.NoError(t, err)
	assert.Len(t, fires, 2)
	assert.Len(t, fires[testdata.Org1.ID], 2)
	assert.Equal(t, testdata.Cathy.ID, fires[testdata.Org1.ID][0].ContactID)
	assert.Equal(t, models.SessionID(1234), fires[testdata.Org1.ID][0].Extra.V.SessionID)
	assert.Len(t, fires[testdata.Org2.ID], 1)

	err = models.DeleteContactFires(ctx, rt, []*models.ContactFire{fires[testdata.Org1.ID][0], fires[testdata.Org1.ID][1]})
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM contacts_contactfire`, 2)
}
