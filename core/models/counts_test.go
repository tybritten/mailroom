package models_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDailyCounts(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	err = models.InsertDailyCounts(ctx, rt.DB, oa, map[string]int{"foo": 1, "bar": 2})
	assert.NoError(t, err)
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM orgs_dailycount`).Returns(2)
	assertdb.Query(t, rt.DB, `SELECT SUM(count) FROM orgs_dailycount WHERE org_id = $1 AND scope = 'foo'`, testdata.Org1.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT SUM(count) FROM orgs_dailycount WHERE org_id = $1 AND scope = 'bar'`, testdata.Org1.ID).Returns(2)
}
