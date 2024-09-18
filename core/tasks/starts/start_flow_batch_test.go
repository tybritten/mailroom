package starts_test

import (
	"testing"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/utils/queues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartFlowBatchTask(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData)

	// create a start object
	start1 := models.NewFlowStart(models.OrgID(1), models.StartTypeManual, testdata.SingleMessage.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.George.ID, testdata.Alexandria.ID})
	err := models.InsertFlowStarts(ctx, rt.DB, []*models.FlowStart{start1})
	require.NoError(t, err)

	batch1 := start1.CreateBatch([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}, false, 4)
	batch2 := start1.CreateBatch([]models.ContactID{testdata.George.ID, testdata.Alexandria.ID}, true, 4)

	// start the first batch...
	err = tasks.Queue(rc, tasks.ThrottledQueue, testdata.Org1.ID, &starts.StartFlowBatchTask{FlowStartBatch: batch1}, queues.DefaultPriority)
	assert.NoError(t, err)
	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = ANY($1) 
		AND status = 'C' AND responded = FALSE AND org_id = 1 AND call_id IS NULL AND output IS NOT NULL`, pq.Array([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})).
		Returns(2)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE contact_id = ANY($1) and flow_id = $2 AND responded = FALSE AND org_id = 1 AND status = 'C'
		AND results IS NOT NULL AND path IS NOT NULL AND session_id IS NOT NULL`, pq.Array([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}), testdata.SingleMessage.ID).
		Returns(2)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = ANY($1) AND text = 'Hey, how are you?' AND org_id = 1 AND status = 'Q' 
		AND direction = 'O' AND msg_type = 'T'`, pq.Array([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})).
		Returns(2)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowstart WHERE id = $1`, start1.ID).Returns("P")

	// start the second and final batch...
	err = tasks.Queue(rc, tasks.ThrottledQueue, testdata.Org1.ID, &starts.StartFlowBatchTask{FlowStartBatch: batch2}, queues.DefaultPriority)
	assert.NoError(t, err)
	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT status FROM flows_flowstart WHERE id = $1`, start1.ID).Returns("C")
}
