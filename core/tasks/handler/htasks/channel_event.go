package htasks

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

const TypeChannelEvent = "channel_event"

func init() {
	handler.RegisterTaskType(TypeChannelEvent, func() handler.Task { return &ChannelEventTask{} })

	// courier still sends these with event_type as task type
	handler.RegisterTaskType("new_conversation", func() handler.Task { return &ChannelEventTask{} })
	handler.RegisterTaskType("referral", func() handler.Task { return &ChannelEventTask{} })
	handler.RegisterTaskType("welcome_message", func() handler.Task { return &ChannelEventTask{} })
	handler.RegisterTaskType("optin", func() handler.Task { return &ChannelEventTask{} })
	handler.RegisterTaskType("optout", func() handler.Task { return &ChannelEventTask{} })
	handler.RegisterTaskType("stop_contact", func() handler.Task { return &ChannelEventTask{} })

	// rapidpro still sends this with event_type as task type
	handler.RegisterTaskType("mo_miss", func() handler.Task { return &ChannelEventTask{} })
}

type ChannelEventTask struct {
	EventID    models.ChannelEventID   `json:"event_id"`
	EventType  models.ChannelEventType `json:"event_type"`
	ChannelID  models.ChannelID        `json:"channel_id"`
	URNID      models.URNID            `json:"urn_id"`
	OptInID    models.OptInID          `json:"optin_id"`
	Extra      null.Map[any]           `json:"extra"`
	NewContact bool                    `json:"new_contact"`
	CreatedOn  time.Time               `json:"created_on"`
}

func (t *ChannelEventTask) Type() string {
	return TypeChannelEvent
}

func (t *ChannelEventTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	_, err := t.handle(ctx, rt, oa, contactID, nil)
	return err
}

// Handle let's us reuse this task's code for handling incoming calls.. which we need to perform inline in the IVR web
// handler rather than as a queued task.
func (t *ChannelEventTask) Handle(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID, call *models.Call) (*models.Session, error) {
	return t.handle(ctx, rt, oa, contactID, call)
}

func (t *ChannelEventTask) handle(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID, call *models.Call) (*models.Session, error) {
	channel := oa.ChannelByID(t.ChannelID)
	if channel == nil {
		slog.Info("ignoring event, couldn't find channel", "channel_id", t.ChannelID)
		return nil, nil
	}

	// load our contact
	modelContact, err := models.LoadContact(ctx, rt.ReadonlyDB, oa, contactID)
	if err != nil {
		if err == sql.ErrNoRows { // if contact no longer exists, ignore event
			return nil, nil
		}
		return nil, errors.Wrapf(err, "error loading contact")
	}

	// if contact is blocked, ignore event
	if modelContact.Status() == models.ContactStatusBlocked {
		return nil, nil
	}

	if t.EventType == models.EventTypeStopContact {
		err = models.StopContact(ctx, rt.DB, oa.OrgID(), contactID)
		if err != nil {
			return nil, errors.Wrapf(err, "error stopping contact")
		}
	}

	if models.ContactSeenEvents[t.EventType] {
		err = modelContact.UpdateLastSeenOn(ctx, rt.DB, t.CreatedOn)
		if err != nil {
			return nil, errors.Wrap(err, "error updating contact last_seen_on")
		}
	}

	// make sure this URN is our highest priority (this is usually a noop)
	err = modelContact.UpdatePreferredURN(ctx, rt.DB, oa, t.URNID, channel)
	if err != nil {
		return nil, errors.Wrapf(err, "error changing primary URN")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating flow contact")
	}

	if t.NewContact {
		err = models.CalculateDynamicGroups(ctx, rt.DB, oa, []*flows.Contact{contact})
		if err != nil {
			return nil, errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// do we have associated trigger?
	var trigger *models.Trigger

	switch t.EventType {
	case models.EventTypeNewConversation:
		trigger = models.FindMatchingNewConversationTrigger(oa, channel)
	case models.EventTypeReferral:
		referrerID, _ := t.Extra["referrer_id"].(string)
		trigger = models.FindMatchingReferralTrigger(oa, channel, referrerID)
	case models.EventTypeMissedCall:
		trigger = models.FindMatchingMissedCallTrigger(oa, channel)
	case models.EventTypeIncomingCall:
		trigger = models.FindMatchingIncomingCallTrigger(oa, channel, contact)
	case models.EventTypeOptIn:
		trigger = models.FindMatchingOptInTrigger(oa, channel)
	case models.EventTypeOptOut:
		trigger = models.FindMatchingOptOutTrigger(oa, channel)
	case models.EventTypeWelcomeMessage, models.EventTypeStopContact:
		trigger = nil
	default:
		return nil, errors.Errorf("unknown channel event type: %s", t.EventType)
	}

	// no trigger then nothing more to do
	if trigger == nil {
		return nil, nil
	}

	// load our flow
	flow, err := oa.FlowByID(trigger.FlowID())
	if err == models.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow for trigger")
	}

	// if this is an IVR flow and we don't have a call, trigger that asynchronously
	if flow.FlowType() == models.FlowTypeVoice && call == nil {
		err = handler.TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{modelContact.ID()}, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "error while triggering ivr flow")
		}
		return nil, nil
	}

	// create our parameters, we just convert this from JSON
	var params *types.XObject
	if t.Extra != nil {
		asJSON, err := json.Marshal(map[string]any(t.Extra))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to marshal extra from channel event")
		}
		params, err = types.ReadXObject(asJSON)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read extra from channel event")
		}
	}

	var flowOptIn *flows.OptIn
	if t.EventType == models.EventTypeOptIn || t.EventType == models.EventTypeOptOut {
		optIn := oa.OptInByID(t.OptInID)
		if optIn != nil {
			flowOptIn = oa.SessionAssets().OptIns().Get(optIn.UUID())
		}
	}

	// build our flow trigger
	tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact)
	var trig flows.Trigger

	if t.EventType == models.EventTypeIncomingCall {
		urn := modelContact.URNForID(t.URNID)
		trig = tb.Channel(channel.Reference(), triggers.ChannelEventTypeIncomingCall).WithCall(urn).Build()
	} else if t.EventType == models.EventTypeOptIn && flowOptIn != nil {
		trig = tb.OptIn(flowOptIn, triggers.OptInEventTypeStarted).Build()
	} else if t.EventType == models.EventTypeOptOut && flowOptIn != nil {
		trig = tb.OptIn(flowOptIn, triggers.OptInEventTypeStopped).Build()
	} else {
		trig = tb.Channel(channel.Reference(), triggers.ChannelEventType(t.EventType)).WithParams(params).Build()
	}

	// if we have a channel connection we set the connection on the session before our event hooks fire
	// so that IVR messages can be created with the right connection reference
	var hook models.SessionCommitHook
	if flow.FlowType() == models.FlowTypeVoice && call != nil {
		hook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
			for _, session := range sessions {
				session.SetCall(call)
			}
			return nil
		}
	}

	sessions, err := runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{modelContact}, []flows.Trigger{trig}, hook, flow.FlowType().Interrupts())
	if err != nil {
		return nil, errors.Wrapf(err, "error starting flow for contact")
	}
	if len(sessions) == 0 {
		return nil, nil
	}
	return sessions[0], nil
}
