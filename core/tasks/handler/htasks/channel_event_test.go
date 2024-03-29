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
	"github.com/nyaruka/null/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// schedule an event for cathy and george
	testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, time.Now())
	testdata.InsertEventFire(rt, testdata.George, testdata.RemindersEvent1, time.Now())

	// and george to doctors group, cathy is already part of it
	rt.DB.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2);`, testdata.DoctorsGroup.ID, testdata.George.ID)

	// add some channel event triggers
	testdata.InsertNewConversationTrigger(rt, testdata.Org1, testdata.Favorites, testdata.FacebookChannel)
	testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.PickANumber, "", testdata.VonageChannel)
	testdata.InsertOptInTrigger(rt, testdata.Org1, testdata.Favorites, testdata.VonageChannel)
	testdata.InsertOptOutTrigger(rt, testdata.Org1, testdata.PickANumber, testdata.VonageChannel)

	polls := testdata.InsertOptIn(rt, testdata.Org1, "Polls")

	// add a URN for cathy so we can test twitter URNs
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Bob, urns.URN("twitterid:123456"), 10, nil)

	// create a deleted contact
	del := testdata.InsertContact(rt, testdata.Org1, "", "Del", "eng", models.ContactStatusActive)
	rt.DB.MustExec(`UPDATE contacts_contact SET is_active = false WHERE id = $1`, del.ID)

	tcs := []struct {
		contact             *testdata.Contact
		task                handler.Task
		expectedTriggerType string
		expectedResponse    string
		updatesLastSeen     bool
	}{
		{ // 0: new conversation on Facebook
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeNewConversation,
				ChannelID:  testdata.FacebookChannel.ID,
				URNID:      testdata.Cathy.URNID,
				Extra:      null.Map[any]{},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "channel",
			expectedResponse:    "What is your favorite color?",
			updatesLastSeen:     true,
		},
		{ // 1: new conversation on Vonage (no trigger)
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeNewConversation,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      testdata.Cathy.URNID,
				Extra:      null.Map[any]{},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "",
			expectedResponse:    "",
			updatesLastSeen:     true,
		},
		{ // 2: welcome message on Vonage
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeWelcomeMessage,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      testdata.Cathy.URNID,
				Extra:      null.Map[any]{},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "",
			expectedResponse:    "",
			updatesLastSeen:     false,
		},
		{ // 3: referral on Facebook
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeReferral,
				ChannelID:  testdata.FacebookChannel.ID,
				URNID:      testdata.Cathy.URNID,
				Extra:      null.Map[any]{},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "",
			expectedResponse:    "",
			updatesLastSeen:     true,
		},
		{ // 4: referral on Facebook
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeReferral,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      testdata.Cathy.URNID,
				Extra:      null.Map[any]{},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "channel",
			expectedResponse:    "Pick a number between 1-10.",
			updatesLastSeen:     true,
		},
		{ // 5: optin on Vonage
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeOptIn,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      testdata.Cathy.URNID,
				OptInID:    polls.ID,
				Extra:      map[string]any{"title": "Polls", "payload": fmt.Sprint(polls.ID)},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "optin",
			expectedResponse:    "What is your favorite color?",
			updatesLastSeen:     true,
		},
		{ // 6: optout on Vonage
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeOptOut,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      testdata.Cathy.URNID,
				OptInID:    polls.ID,
				Extra:      map[string]any{"title": "Polls", "payload": fmt.Sprint(polls.ID)},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "optin",
			expectedResponse:    "Pick a number between 1-10.",
			updatesLastSeen:     true,
		},
		{ // 7: missed call trigger queued by RP
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeMissedCall,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      testdata.Cathy.URNID,
				OptInID:    polls.ID,
				Extra:      map[string]any{"duration": 123},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "",
			expectedResponse:    "",
			updatesLastSeen:     true,
		},
		{ // 8: stop contact
			contact: testdata.Cathy,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeStopContact,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      testdata.Cathy.URNID,
				Extra:      null.Map[any]{},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "",
			expectedResponse:    "",
			updatesLastSeen:     true,
		},
		{ // 9: a task against a deleted contact
			contact: del,
			task: &htasks.ChannelEventTask{
				EventType:  models.EventTypeNewConversation,
				ChannelID:  testdata.VonageChannel.ID,
				URNID:      del.URNID,
				Extra:      null.Map[any]{},
				CreatedOn:  time.Now(),
				NewContact: false,
			},
			expectedTriggerType: "",
			expectedResponse:    "",
			updatesLastSeen:     false,
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
			if assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND created_on > $2`, tc.contact.ID, start).Returns(1, "%d: expected new session", i) {
				// get session output to lookup trigger type
				var output []byte
				err = rt.DB.Get(&output, `SELECT output FROM flows_flowsession WHERE contact_id = $1 AND created_on > $2`, tc.contact.ID, start)
				require.NoError(t, err)

				trigType, err := jsonparser.GetString(output, "trigger", "type")
				require.NoError(t, err)
				assert.Equal(t, tc.expectedTriggerType, trigType)
			}

			assertdb.Query(t, rt.DB, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.contact.ID, start).
				Returns(tc.expectedResponse, "%d: response mismatch", i)
		}

		if tc.updatesLastSeen {
			var lastSeen time.Time
			err = rt.DB.Get(&lastSeen, `SELECT last_seen_on FROM contacts_contact WHERE id = $1`, tc.contact.ID)
			assert.NoError(t, err)
			assert.WithinDuration(t, lastSeen, tc.task.(*htasks.ChannelEventTask).CreatedOn, time.Microsecond, "%d: expected last seen to be updated", i)
		}
	}

	// last event was a stop_contact so check that cathy is stopped
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(1)

	// and that only george is left in the group
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.George.ID).Returns(1)

	// and she has no upcoming events
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.George.ID).Returns(1)
}
