package llm_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestTranslate(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	testsuite.RunWebTests(t, ctx, rt, "testdata/translate.json", map[string]string{
		"test_llm_id": fmt.Sprint(testdata.TestLLM.ID),
	})
}
