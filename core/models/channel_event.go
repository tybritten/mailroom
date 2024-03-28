package models

import (
	"encoding/json"
	"time"

	"github.com/nyaruka/null/v3"
)

type ChannelEventType string
type ChannelEventID int64

// channel event types
const (
	EventTypeNewConversation ChannelEventType = "new_conversation"
	EventTypeWelcomeMessage  ChannelEventType = "welcome_message"
	EventTypeReferral        ChannelEventType = "referral"
	EventTypeMissedCall      ChannelEventType = "mo_miss"
	EventTypeIncomingCall    ChannelEventType = "mo_call"
	EventTypeStopContact     ChannelEventType = "stop_contact"
	EventTypeOptIn           ChannelEventType = "optin"
	EventTypeOptOut          ChannelEventType = "optout"
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
	e struct {
		ID         ChannelEventID   `json:"id"           db:"id"`
		EventType  ChannelEventType `json:"event_type"   db:"event_type"`
		OrgID      OrgID            `json:"org_id"       db:"org_id"`
		ChannelID  ChannelID        `json:"channel_id"   db:"channel_id"`
		ContactID  ContactID        `json:"contact_id"   db:"contact_id"`
		URNID      URNID            `json:"urn_id"       db:"contact_urn_id"`
		OptInID    OptInID          `json:"optin_id"     db:"optin_id"`
		Extra      null.Map[any]    `json:"extra"        db:"extra"`
		OccurredOn time.Time        `json:"occurred_on"  db:"occurred_on"`
		CreatedOn  time.Time        `json:"created_on"   db:"created_on"`

		// only in JSON representation
		NewContact bool `json:"new_contact"`
	}
}

func (e *ChannelEvent) ID() ChannelEventID    { return e.e.ID }
func (e *ChannelEvent) ContactID() ContactID  { return e.e.ContactID }
func (e *ChannelEvent) URNID() URNID          { return e.e.URNID }
func (e *ChannelEvent) OrgID() OrgID          { return e.e.OrgID }
func (e *ChannelEvent) ChannelID() ChannelID  { return e.e.ChannelID }
func (e *ChannelEvent) IsNewContact() bool    { return e.e.NewContact }
func (e *ChannelEvent) OccurredOn() time.Time { return e.e.OccurredOn }
func (e *ChannelEvent) CreatedOn() time.Time  { return e.e.CreatedOn }
func (e *ChannelEvent) OptInID() OptInID      { return e.e.OptInID }
func (e *ChannelEvent) Extra() map[string]any { return e.e.Extra }
func (e *ChannelEvent) ExtraString(key string) string {
	asStr, ok := e.e.Extra[key].(string)
	if ok {
		return asStr
	}
	return ""
}

// MarshalJSON is our custom marshaller so that our inner struct get output
func (e *ChannelEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.e)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (e *ChannelEvent) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &e.e)
}
