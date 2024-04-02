package models

import (
	"context"
	"time"

	"github.com/nyaruka/null/v3"
)

type ChannelEventID int64
type ChannelEventType string
type ChannelEventStatus string

const (
	NilChannelEventID ChannelEventID = 0

	// channel event types
	EventTypeNewConversation ChannelEventType = "new_conversation"
	EventTypeWelcomeMessage  ChannelEventType = "welcome_message"
	EventTypeReferral        ChannelEventType = "referral"
	EventTypeMissedCall      ChannelEventType = "mo_miss"
	EventTypeIncomingCall    ChannelEventType = "mo_call"
	EventTypeStopContact     ChannelEventType = "stop_contact"
	EventTypeOptIn           ChannelEventType = "optin"
	EventTypeOptOut          ChannelEventType = "optout"

	// channel event statuses
	EventStatusPending ChannelEventStatus = "P" // event created but not yet handled
	EventStatusHandled ChannelEventStatus = "H" // event handled
)

// ContactSeenEvents are those which count as the contact having been seen
var ContactSeenEvents = map[ChannelEventType]bool{
	EventTypeNewConversation: true,
	EventTypeReferral:        true,
	EventTypeMissedCall:      true,
	EventTypeIncomingCall:    true,
	EventTypeStopContact:     true,
	EventTypeOptIn:           true,
	EventTypeOptOut:          true,
}

// ChannelEvent represents an event that occurred associated with a channel, such as a referral, missed call, etc..
type ChannelEvent struct {
	ID         ChannelEventID   `db:"id"`
	EventType  ChannelEventType `db:"event_type"`
	OrgID      OrgID            `db:"org_id"`
	ChannelID  ChannelID        `db:"channel_id"`
	ContactID  ContactID        `db:"contact_id"`
	URNID      URNID            `db:"contact_urn_id"`
	OptInID    OptInID          `db:"optin_id"`
	Extra      null.Map[any]    `db:"extra"`
	OccurredOn time.Time        `db:"occurred_on"`
	CreatedOn  time.Time        `db:"created_on"`
}

const sqlInsertChannelEvent = `
INSERT INTO channels_channelevent( org_id,  event_type,  channel_id,  contact_id,  contact_urn_id,  optin_id,  extra, created_on, occurred_on)
	                       VALUES(:org_id, :event_type, :channel_id, :contact_id, :contact_urn_id, :optin_id, :extra, NOW(),     :occurred_on)
  RETURNING id, created_on`

// Insert inserts this channel event to our DB. The ID of the channel event will be
// set if no error is returned
func (e *ChannelEvent) Insert(ctx context.Context, db DBorTx) error {
	return BulkQuery(ctx, "insert channel event", db, sqlInsertChannelEvent, []any{e})
}

// NewChannelEvent creates a new channel event for the passed in parameters, returning it
func NewChannelEvent(eventType ChannelEventType, orgID OrgID, channelID ChannelID, contactID ContactID, urnID URNID, extra map[string]any, occurredOn time.Time) *ChannelEvent {
	e := &ChannelEvent{
		EventType:  eventType,
		OrgID:      orgID,
		ChannelID:  channelID,
		ContactID:  contactID,
		URNID:      urnID,
		OccurredOn: occurredOn,
	}

	if extra == nil {
		e.Extra = null.Map[any]{}
	} else {
		e.Extra = null.Map[any](extra)
	}

	return e
}
