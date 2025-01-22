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

func TestBulkSessionExpire(t *testing.T) {
	_, rt := testsuite.Runtime()
	defer testsuite.Reset(testsuite.ResetRedis)

	defer dates.SetNowFunc(time.Now)
	dates.SetNowFunc(dates.NewFixedNow(time.Date(2024, 11, 15, 13, 59, 0, 0, time.UTC)))

	testsuite.QueueBatchTask(t, rt, testdata.Org1, &contacts.BulkSessionExpireTask{
		Expirations: []*contacts.Expiration{
			{
				ContactID:  testdata.Cathy.ID,
				SessionID:  123456,
				ModifiedOn: time.Date(2024, 11, 15, 13, 57, 0, 0, time.UTC),
			},
			{
				ContactID:  testdata.Bob.ID,
				SessionID:  234567,
				ModifiedOn: time.Date(2024, 11, 15, 13, 58, 0, 0, time.UTC),
			},
		},
	})

	assert.Equal(t, map[string]int{"bulk_session_expire": 1}, testsuite.FlushTasks(t, rt, "batch", "throttled"))

	testsuite.AssertContactTasks(t, testdata.Org1, testdata.Cathy, []string{
		`{"type":"expiration_event","task":{"session_id":123456,"modified_on":"2024-11-15T13:57:00Z"},"queued_on":"2024-11-15T13:59:00Z"}`,
	})
	testsuite.AssertContactTasks(t, testdata.Org1, testdata.Bob, []string{
		`{"type":"expiration_event","task":{"session_id":234567,"modified_on":"2024-11-15T13:58:00Z"},"queued_on":"2024-11-15T13:59:00Z"}`,
	})
}
