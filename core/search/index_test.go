package search_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/elastic"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeindexContactsByID(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	testsuite.ReindexElastic(ctx)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contact WHERE org_id = $1`, testdata.Org1.ID).Returns(124)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contact WHERE org_id = $1`, testdata.Org2.ID).Returns(121)
	assertSearchCount(t, rt, elastic.Term("org_id", testdata.Org1.ID), 124)
	assertSearchCount(t, rt, elastic.Term("org_id", testdata.Org2.ID), 121)

	// try to deindex contacts which aren't deleted
	deindexed, err := search.DeindexContactsByID(ctx, rt, []models.ContactID{testdata.Bob.ID, testdata.George.ID})
	assert.NoError(t, err)
	assert.Equal(t, 0, deindexed)

	assertSearchCount(t, rt, elastic.Term("org_id", testdata.Org1.ID), 124)
	assertSearchCount(t, rt, elastic.Term("org_id", testdata.Org2.ID), 121)

	rt.DB.MustExec(`UPDATE contacts_contact SET is_active = false WHERE org_id = $1`, testdata.Org1.ID)

	deindexed, err = search.DeindexContactsByID(ctx, rt, []models.ContactID{testdata.Bob.ID, testdata.George.ID})
	assert.NoError(t, err)
	assert.Equal(t, 2, deindexed)

	assertSearchCount(t, rt, elastic.Term("org_id", testdata.Org1.ID), 122)
	assertSearchCount(t, rt, elastic.Term("org_id", testdata.Org2.ID), 121)
}

func assertSearchCount(t *testing.T, rt *runtime.Runtime, query elastic.Query, expected int) {
	src := map[string]any{"query": query}

	resp, err := rt.ES.Count().Index(rt.Config.ElasticContactsIndex).Raw(bytes.NewReader(jsonx.MustMarshal(src))).Do(context.Background())
	require.NoError(t, err)
	assert.Equal(t, expected, int(resp.Count))
}
