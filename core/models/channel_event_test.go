package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/null/v3"
	"github.com/stretchr/testify/assert"
)

func TestChannelEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer rt.DB.MustExec(`DELETE FROM channels_channelevent`)

	// no extra
	e := models.NewChannelEvent(models.EventTypeMissedCall, testdata.Org1.ID, testdata.TwilioChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, nil, time.Date(2024, 4, 1, 15, 13, 45, 0, time.UTC))
	err := e.Insert(ctx, rt.DB)
	assert.NoError(t, err)
	assert.NotZero(t, e.ID)
	assert.Equal(t, null.Map[any]{}, e.Extra)
	assert.True(t, e.OccurredOn.Equal(time.Date(2024, 4, 1, 15, 13, 45, 0, time.UTC)))

	// with extra
	e2 := models.NewChannelEvent(models.EventTypeMissedCall, testdata.Org1.ID, testdata.TwilioChannel.ID, testdata.Cathy.ID, testdata.Cathy.URNID, map[string]any{"referral_id": "foobar"}, time.Date(2024, 4, 1, 15, 13, 45, 0, time.UTC))
	err = e2.Insert(ctx, rt.DB)
	assert.NoError(t, err)
	assert.NotZero(t, e2.ID)
	assert.Equal(t, null.Map[any]{"referral_id": "foobar"}, e2.Extra)

	asJSON, err := json.Marshal(e2)
	assert.NoError(t, err)

	e3 := &models.ChannelEvent{}
	err = json.Unmarshal(asJSON, e3)
	assert.NoError(t, err)
	assert.Equal(t, e2.Extra, e3.Extra)
	assert.True(t, e.OccurredOn.Equal(time.Date(2024, 4, 1, 15, 13, 45, 0, time.UTC)))
}
