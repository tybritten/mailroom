package interrupts_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/interrupts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestInterrupts(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa := testdata.Org1.Load(rt)

	tcs := []struct {
		contactIDs       []models.ContactID
		flowIDs          []models.FlowID
		expectedStatuses [4]string
	}{
		{
			contactIDs:       nil,
			flowIDs:          nil,
			expectedStatuses: [4]string{"W", "W", "W", "W"},
		},
		{
			contactIDs:       []models.ContactID{testdata.Cathy.ID},
			flowIDs:          nil,
			expectedStatuses: [4]string{"I", "W", "W", "W"},
		},
		{
			contactIDs:       []models.ContactID{testdata.Cathy.ID, testdata.George.ID},
			flowIDs:          nil,
			expectedStatuses: [4]string{"I", "I", "W", "W"},
		},
		{
			contactIDs:       nil,
			flowIDs:          []models.FlowID{testdata.PickANumber.ID},
			expectedStatuses: [4]string{"W", "W", "W", "I"},
		},
		{
			contactIDs:       []models.ContactID{testdata.Cathy.ID, testdata.George.ID},
			flowIDs:          []models.FlowID{testdata.PickANumber.ID},
			expectedStatuses: [4]string{"I", "I", "W", "I"},
		},
	}

	for i, tc := range tcs {
		// mark any remaining flow sessions as inactive
		rt.DB.MustExec(`UPDATE flows_flowsession SET status='C', ended_on=NOW() WHERE status = 'W';`)

		// twilio call
		twilioCallID := testdata.InsertCall(rt, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria)

		sessionIDs := make([]models.SessionID, 4)

		// insert our dummy contact sessions
		sessionIDs[0], _ = testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID)
		sessionIDs[1], _ = testdata.InsertWaitingSession(rt, testdata.Org1, testdata.George, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID)
		sessionIDs[2], _ = testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Alexandria, models.FlowTypeVoice, testdata.Favorites, twilioCallID)
		sessionIDs[3], _ = testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.PickANumber, models.NilCallID)

		// create our task
		task := &interrupts.InterruptSessionsTask{
			ContactIDs: tc.contactIDs,
			FlowIDs:    tc.flowIDs,
		}

		// execute it
		err := task.Perform(ctx, rt, oa)
		assert.NoError(t, err)

		// check session statuses are as expected
		for j, sID := range sessionIDs {
			var status string
			err := rt.DB.Get(&status, `SELECT status FROM flows_flowsession WHERE id = $1`, sID)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedStatuses[j], status, "%d: status mismatch for session #%d", i, j)

			// check for runs with a different status to the session
			assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE session_id = $1 AND status != $2`, sID, tc.expectedStatuses[j]).
				Returns(0, "%d: unexpected un-interrupted runs for session #%d", i, j)
		}
	}
}
