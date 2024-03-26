package htasks_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/htasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// add some channel event triggers
	testdata.InsertNewConversationTrigger(rt, testdata.Org1, testdata.Favorites, testdata.FacebookChannel)
	testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.PickANumber, "", testdata.VonageChannel)
	testdata.InsertOptInTrigger(rt, testdata.Org1, testdata.Favorites, testdata.VonageChannel)
	testdata.InsertOptOutTrigger(rt, testdata.Org1, testdata.PickANumber, testdata.VonageChannel)

	polls := testdata.InsertOptIn(rt, testdata.Org1, "Polls")

	// add a URN for cathy so we can test twitter URNs
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Bob, urns.URN("twitterid:123456"), 10, nil)

	tcs := []struct {
		contact             *testdata.Contact
		task                handler.Task
		expectedTriggerType string
		expectedResponse    string
		updatesLastSeen     bool
	}{
		{
			testdata.Cathy,
			&htasks.NewConversationTask{
				ChannelEvent: models.NewChannelEvent(
					models.EventTypeNewConversation, testdata.Org1.ID, testdata.FacebookChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, models.NilOptInID, nil, false,
				),
			},
			"channel",
			"What is your favorite color?",
			true,
		},
		{
			testdata.Cathy,
			&htasks.NewConversationTask{
				ChannelEvent: models.NewChannelEvent(
					models.EventTypeNewConversation, testdata.Org1.ID, testdata.VonageChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, models.NilOptInID, nil, false,
				),
			},
			"",
			"",
			true,
		},
		{
			testdata.Cathy,
			&htasks.WelcomeMessageTask{
				ChannelEvent: models.NewChannelEvent(
					models.EventTypeWelcomeMessage, testdata.Org1.ID, testdata.VonageChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, models.NilOptInID, nil, false,
				),
			},
			"",
			"",
			false,
		},
		{
			testdata.Cathy,
			&htasks.ReferralTask{
				ChannelEvent: models.NewChannelEvent(
					models.EventTypeReferral, testdata.Org1.ID, testdata.FacebookChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, models.NilOptInID, nil, false,
				),
			},
			"",
			"",
			true,
		},
		{
			testdata.Cathy,
			&htasks.ReferralTask{
				ChannelEvent: models.NewChannelEvent(
					models.EventTypeReferral, testdata.Org1.ID, testdata.VonageChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, models.NilOptInID, nil, false,
				),
			},
			"channel",
			"Pick a number between 1-10.",
			true,
		},
		{
			testdata.Cathy,
			&htasks.OptInTask{
				ChannelEvent: models.NewChannelEvent(
					models.EventTypeOptIn, testdata.Org1.ID, testdata.VonageChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, polls.ID, map[string]any{"title": "Polls", "payload": fmt.Sprint(polls.ID)}, false,
				),
			},
			"optin",
			"What is your favorite color?",
			true,
		},
		{
			testdata.Cathy,
			&htasks.OptOutTask{
				ChannelEvent: models.NewChannelEvent(
					models.EventTypeOptOut, testdata.Org1.ID, testdata.VonageChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, polls.ID, map[string]any{"title": "Polls", "payload": fmt.Sprint(polls.ID)}, false,
				),
			},
			"optin",
			"Pick a number between 1-10.",
			true,
		},
	}

	models.FlushCache()

	for i, tc := range tcs {
		start := time.Now()
		time.Sleep(time.Millisecond * 5)

		err := handler.QueueTask(rc, testdata.Org1.ID, tc.contact.ID, tc.task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err := tasks.HandlerQueue.Pop(rc)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = tasks.Perform(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to trigger a new session...
		if tc.expectedTriggerType != "" {
			assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND created_on > $2`, tc.contact.ID, start).Returns(1)

			var output []byte
			err = rt.DB.Get(&output, `SELECT output FROM flows_flowsession WHERE contact_id = $1 AND created_on > $2`, tc.contact.ID, start)
			require.NoError(t, err)

			trigType, err := jsonparser.GetString(output, "trigger", "type")
			require.NoError(t, err)
			assert.Equal(t, tc.expectedTriggerType, trigType)

			assertdb.Query(t, rt.DB, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.contact.ID, start).
				Returns(tc.expectedResponse, "%d: response mismatch", i)
		}

		if tc.updatesLastSeen {
			var lastSeen time.Time
			err = rt.DB.Get(&lastSeen, `SELECT last_seen_on FROM contacts_contact WHERE id = $1`, tc.contact.ID)
			assert.NoError(t, err)
			assert.True(t, lastSeen.Equal(start) || lastSeen.After(start), "%d: expected last seen to be updated", i)
		}
	}
}
