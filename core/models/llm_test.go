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
		{testdata.OpenAI1.ID, testdata.OpenAI1.UUID, "GPT-3.5 Turbo!", "openai"},
		{testdata.OpenAI2.ID, testdata.OpenAI2.UUID, "GPT-4", "openai"},
	}

	assert.Equal(t, len(tcs), len(llms))
	for i, tc := range tcs {
		c := llms[i].(*models.LLM)
		assert.Equal(t, tc.uuid, c.UUID())
		assert.Equal(t, tc.id, c.ID())
		assert.Equal(t, tc.name, c.Name())
		assert.Equal(t, tc.typ, c.Type())
	}

	assert.Equal(t, "GPT-4", oa.LLMByUUID(testdata.OpenAI2.UUID).Name())
	assert.Nil(t, oa.LLMByUUID("5e9d8fab-5e7e-4f51-b533-261af5dea70d"))
}
