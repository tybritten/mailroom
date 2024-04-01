package android_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

func TestEvent(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	testsuite.RunWebTests(t, ctx, rt, "testdata/event.json", nil)
}

func TestMessage(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	testsuite.RunWebTests(t, ctx, rt, "testdata/message.json", nil)
}
