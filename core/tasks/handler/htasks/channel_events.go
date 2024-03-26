package htasks

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

// TODO it would be simpler to model these as a single channel_event task type but courier queues them with different
// top-level task types, and no type on the actual event payload itself.

func init() {
	handler.RegisterTaskType(string(models.EventTypeNewConversation), func() handler.Task { return &NewConversationTask{ChannelEvent: &models.ChannelEvent{}} })
	handler.RegisterTaskType(string(models.EventTypeReferral), func() handler.Task { return &ReferralTask{ChannelEvent: &models.ChannelEvent{}} })
	handler.RegisterTaskType(string(models.EventTypeMissedCall), func() handler.Task { return &MissedCallTask{ChannelEvent: &models.ChannelEvent{}} })
	handler.RegisterTaskType(string(models.EventTypeWelcomeMessage), func() handler.Task { return &WelcomeMessageTask{ChannelEvent: &models.ChannelEvent{}} })
	handler.RegisterTaskType(string(models.EventTypeOptIn), func() handler.Task { return &OptInTask{ChannelEvent: &models.ChannelEvent{}} })
	handler.RegisterTaskType(string(models.EventTypeOptOut), func() handler.Task { return &OptOutTask{ChannelEvent: &models.ChannelEvent{}} })
}

type NewConversationTask struct {
	*models.ChannelEvent
}

func (t *NewConversationTask) Type() string {
	return string(models.EventTypeNewConversation)
}

func (t *NewConversationTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	_, err := HandleChannelEvent(ctx, rt, oa, models.ChannelEventType(t.Type()), t.ChannelEvent, nil)
	return err
}

type ReferralTask struct {
	*models.ChannelEvent
}

func (t *ReferralTask) Type() string {
	return string(models.EventTypeReferral)
}

func (t *ReferralTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	_, err := HandleChannelEvent(ctx, rt, oa, models.ChannelEventType(t.Type()), t.ChannelEvent, nil)
	return err
}

type MissedCallTask struct {
	*models.ChannelEvent
}

func (t *MissedCallTask) Type() string {
	return string(models.EventTypeMissedCall)
}

func (t *MissedCallTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	_, err := HandleChannelEvent(ctx, rt, oa, models.ChannelEventType(t.Type()), t.ChannelEvent, nil)
	return err
}

type WelcomeMessageTask struct {
	*models.ChannelEvent
}

func (t *WelcomeMessageTask) Type() string {
	return string(models.EventTypeWelcomeMessage)
}

func (t *WelcomeMessageTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	_, err := HandleChannelEvent(ctx, rt, oa, models.ChannelEventType(t.Type()), t.ChannelEvent, nil)
	return err
}

type OptInTask struct {
	*models.ChannelEvent
}

func (t *OptInTask) Type() string {
	return string(models.EventTypeOptIn)
}

func (t *OptInTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	_, err := HandleChannelEvent(ctx, rt, oa, models.ChannelEventType(t.Type()), t.ChannelEvent, nil)
	return err
}

type OptOutTask struct {
	*models.ChannelEvent
}

func (t *OptOutTask) Type() string {
	return string(models.EventTypeOptOut)
}

func (t *OptOutTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactID models.ContactID) error {
	_, err := HandleChannelEvent(ctx, rt, oa, models.ChannelEventType(t.Type()), t.ChannelEvent, nil)
	return err
}

// HandleChannelEvent is called for channel events
func HandleChannelEvent(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, eventType models.ChannelEventType, event *models.ChannelEvent, call *models.Call) (*models.Session, error) {
	channel := oa.ChannelByID(event.ChannelID())
	if channel == nil {
		slog.Info("ignoring event, couldn't find channel", "channel_id", event.ChannelID)
		return nil, nil
	}

	// load our contact
	modelContact, err := models.LoadContact(ctx, rt.ReadonlyDB, oa, event.ContactID())
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

	if models.ContactSeenEvents[eventType] {
		// in the case of an incoming call this event isn't in the db and doesn't have created on
		lastSeenOn := event.CreatedOn()
		if lastSeenOn.IsZero() {
			lastSeenOn = dates.Now()
		}

		err = modelContact.UpdateLastSeenOn(ctx, rt.DB, lastSeenOn)
		if err != nil {
			return nil, errors.Wrap(err, "error updating contact last_seen_on")
		}
	}

	// make sure this URN is our highest priority (this is usually a noop)
	err = modelContact.UpdatePreferredURN(ctx, rt.DB, oa, event.URNID(), channel)
	if err != nil {
		return nil, errors.Wrapf(err, "error changing primary URN")
	}

	// build our flow contact
	contact, err := modelContact.FlowContact(oa)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating flow contact")
	}

	// do we have associated trigger?
	var trigger *models.Trigger

	switch eventType {
	case models.EventTypeNewConversation:
		trigger = models.FindMatchingNewConversationTrigger(oa, channel)
	case models.EventTypeReferral:
		trigger = models.FindMatchingReferralTrigger(oa, channel, event.ExtraString("referrer_id"))
	case models.EventTypeMissedCall:
		trigger = models.FindMatchingMissedCallTrigger(oa, channel)
	case models.EventTypeIncomingCall:
		trigger = models.FindMatchingIncomingCallTrigger(oa, channel, contact)
	case models.EventTypeOptIn:
		trigger = models.FindMatchingOptInTrigger(oa, channel)
	case models.EventTypeOptOut:
		trigger = models.FindMatchingOptOutTrigger(oa, channel)
	case models.EventTypeWelcomeMessage:
		trigger = nil
	default:
		return nil, errors.Errorf("unknown channel event type: %s", eventType)
	}

	if event.IsNewContact() {
		err = models.CalculateDynamicGroups(ctx, rt.DB, oa, []*flows.Contact{contact})
		if err != nil {
			return nil, errors.Wrapf(err, "unable to initialize new contact")
		}
	}

	// no trigger, noop, move on
	if trigger == nil {
		slog.Info("ignoring channel event, no trigger found", "channel_id", event.ChannelID(), "event_type", eventType, "extra", event.Extra())
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
	if event.Extra() != nil {
		asJSON, err := json.Marshal(event.Extra())
		if err != nil {
			return nil, errors.Wrapf(err, "unable to marshal extra from channel event")
		}
		params, err = types.ReadXObject(asJSON)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read extra from channel event")
		}
	}

	var flowOptIn *flows.OptIn
	if eventType == models.EventTypeOptIn || eventType == models.EventTypeOptOut {
		optIn := oa.OptInByID(event.OptInID())
		if optIn != nil {
			flowOptIn = oa.SessionAssets().OptIns().Get(optIn.UUID())
		}
	}

	// build our flow trigger
	tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact)
	var trig flows.Trigger

	if eventType == models.EventTypeIncomingCall {
		urn := modelContact.URNForID(event.URNID())
		trig = tb.Channel(channel.Reference(), triggers.ChannelEventTypeIncomingCall).WithCall(urn).Build()
	} else if eventType == models.EventTypeOptIn && flowOptIn != nil {
		trig = tb.OptIn(flowOptIn, triggers.OptInEventTypeStarted).Build()
	} else if eventType == models.EventTypeOptOut && flowOptIn != nil {
		trig = tb.OptIn(flowOptIn, triggers.OptInEventTypeStopped).Build()
	} else {
		trig = tb.Channel(channel.Reference(), triggers.ChannelEventType(eventType)).WithParams(params).Build()
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
