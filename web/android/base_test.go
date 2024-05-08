package android_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestEvent(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	testsuite.RunWebTests(t, ctx, rt, "testdata/event.json", nil)

	orgTasks := testsuite.CurrentTasks(t, rt, "handler")[testdata.Org1.ID]
	assert.Len(t, orgTasks, 1)
	assert.Equal(t, "handle_contact_event", orgTasks[0].Type)
}

func TestMessage(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	testsuite.RunWebTests(t, ctx, rt, "testdata/message.json", nil)

	orgTasks := testsuite.CurrentTasks(t, rt, "handler")[testdata.Org1.ID]
	assert.Len(t, orgTasks, 3)
	assert.Equal(t, "handle_contact_event", orgTasks[0].Type)
	assert.Equal(t, "handle_contact_event", orgTasks[1].Type)
}
