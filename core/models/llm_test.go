package models_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLLMs(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshLLMs)
	require.NoError(t, err)

	llms, err := oa.LLMs()
	require.NoError(t, err)

	tcs := []struct {
		id   models.LLMID
		uuid assets.LLMUUID
		name string
		typ  string
	}{
		{testdata.OpenAI.ID, testdata.OpenAI.UUID, "GPT-4o", "openai"},
		{testdata.Anthropic.ID, testdata.Anthropic.UUID, "Claude", "anthropic"},
		{testdata.TestLLM.ID, testdata.TestLLM.UUID, "Test", "test"},
	}

	assert.Equal(t, len(tcs), len(llms))
	for i, tc := range tcs {
		c := llms[i].(*models.LLM)
		assert.Equal(t, tc.uuid, c.UUID())
		assert.Equal(t, tc.id, c.ID())
		assert.Equal(t, tc.name, c.Name())
		assert.Equal(t, tc.typ, c.Type())
	}

	assert.Equal(t, "Claude", oa.LLMByID(testdata.Anthropic.ID).Name())
	assert.Nil(t, oa.LLMByID(1235))
}
