package handlers_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestOptinRequested(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	optIn := testdata.InsertOptIn(rt, testdata.Org1, "Jokes")
	models.FlushCache()

	rt.DB.MustExec(`UPDATE contacts_contacturn SET identity = 'facebook:12345', scheme='facebook', path='12345' WHERE contact_id = $1`, testdata.Cathy.ID)

	msg1 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "start", models.MsgStatusHandled)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewRequestOptIn(handlers.NewActionUUID(), assets.NewOptInReference(optIn.UUID, "Jokes")),
				},
				testdata.George: []flows.Action{
					actions.NewRequestOptIn(handlers.NewActionUUID(), assets.NewOptInReference(optIn.UUID, "Jokes")),
				},
			},
			Msgs: handlers.ContactMsgMap{
				testdata.Cathy: msg1.FlowMsg,
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `SELECT COUNT(*) FROM msgs_msg WHERE direction = 'O' AND contact_id = $1 AND optin_id = $2`,
					Args:  []any{testdata.Cathy.ID, optIn.ID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)

	assertdb.Query(t, rt.DB, "SELECT text FROM msgs_msg WHERE contact_id = $1", testdata.Cathy.ID).Columns(map[string]any{"text": ""})

	/*rc := rt.RP.Get()
	defer rc.Close()

	// Cathy should have 1 batch of queued messages at high priority
	count, err := redis.Int(rc.Do("zcard", fmt.Sprintf("msgs:%s|10/1", testdata.TwilioChannel.UUID)))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// One bulk for George
	count, err = redis.Int(rc.Do("zcard", fmt.Sprintf("msgs:%s|10/0", testdata.TwilioChannel.UUID)))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)*/
}
