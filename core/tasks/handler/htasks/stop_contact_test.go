package htasks_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/htasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestStopContact(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// schedule an event for cathy and george
	testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, time.Now())
	testdata.InsertEventFire(rt, testdata.George, testdata.RemindersEvent1, time.Now())

	// and george to doctors group, cathy is already part of it
	rt.DB.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2);`, testdata.DoctorsGroup.ID, testdata.George.ID)

	err := handler.QueueTask(rc, testdata.Org1.ID, testdata.Cathy.ID, &htasks.StopContactTask{CreatedOn: time.Now()})
	assert.NoError(t, err, "error adding task")

	task, err := tasks.HandlerQueue.Pop(rc)
	assert.NoError(t, err, "error popping next task")

	err = tasks.Perform(ctx, rt, task)
	assert.NoError(t, err, "error when handling event")

	// check that only george is in our group
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.George.ID).Returns(1)

	// that cathy is stopped
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(1)

	// and has no upcoming events
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.George.ID).Returns(1)
}
