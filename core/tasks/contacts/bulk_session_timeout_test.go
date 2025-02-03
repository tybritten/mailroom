package contacts_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestBulkSessionTimeout(t *testing.T) {
	_, rt := testsuite.Runtime()
	defer testsuite.Reset(testsuite.ResetRedis)

	defer dates.SetNowFunc(time.Now)
	dates.SetNowFunc(dates.NewFixedNow(time.Date(2024, 11, 15, 13, 59, 0, 0, time.UTC)))

	testsuite.QueueBatchTask(t, rt, testdata.Org1, &contacts.BulkSessionTimeoutTask{
		Timeouts: []*contacts.Timeout{
			{
				ContactID:   testdata.Cathy.ID,
				SessionUUID: "8e2786dc-e6d0-4a6a-bbc5-4ec321d60516",
				SprintUUID:  "babdfd9e-241d-4d32-be5f-d821d1ecab31",
				SessionID:   123456,
				ModifiedOn:  time.Date(2024, 11, 15, 13, 57, 0, 0, time.UTC),
			},
			{
				ContactID:   testdata.Bob.ID,
				SessionUUID: "b38dcb5b-9475-423d-a6bf-253b35831f4b",
				SprintUUID:  "c4d1fcc0-ca3f-4b7e-8184-804d039a3d23",
				SessionID:   234567,
				ModifiedOn:  time.Date(2024, 11, 15, 13, 58, 0, 0, time.UTC),
			},
		},
	})

	assert.Equal(t, map[string]int{"bulk_session_timeout": 1}, testsuite.FlushTasks(t, rt, "batch", "throttled"))

	testsuite.AssertContactTasks(t, testdata.Org1, testdata.Cathy, []string{
		`{"type":"timeout_event","task":{"session_uuid":"8e2786dc-e6d0-4a6a-bbc5-4ec321d60516","sprint_uuid":"babdfd9e-241d-4d32-be5f-d821d1ecab31","session_id":123456,"modified_on":"2024-11-15T13:57:00Z"},"queued_on":"2024-11-15T13:59:00Z"}`,
	})
	testsuite.AssertContactTasks(t, testdata.Org1, testdata.Bob, []string{
		`{"type":"timeout_event","task":{"session_uuid":"b38dcb5b-9475-423d-a6bf-253b35831f4b","sprint_uuid":"c4d1fcc0-ca3f-4b7e-8184-804d039a3d23","session_id":234567,"modified_on":"2024-11-15T13:58:00Z"},"queued_on":"2024-11-15T13:59:00Z"}`,
	})
}
