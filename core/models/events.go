package models

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	postCommitTimeout = time.Minute
)

// Scene represents the context that events are occurring in
type Scene struct {
	contact *flows.Contact
	session *Session
	userID  UserID

	preCommits  map[SceneCommitHook][]any
	postCommits map[SceneCommitHook][]any
}

// NewSceneForSession creates a new scene for the passed in session
func NewSceneForSession(session *Session) *Scene {
	return &Scene{
		contact: session.Contact(),
		session: session,

		preCommits:  make(map[SceneCommitHook][]any),
		postCommits: make(map[SceneCommitHook][]any),
	}
}

// NewSceneForContact creates a new scene for the passed in contact, session will be nil
func NewSceneForContact(contact *flows.Contact, userID UserID) *Scene {
	return &Scene{
		contact: contact,
		userID:  userID,

		preCommits:  make(map[SceneCommitHook][]any),
		postCommits: make(map[SceneCommitHook][]any),
	}
}

// SessionUUID returns the session UUID for this scene if any
func (s *Scene) SessionUUID() flows.SessionUUID {
	if s.session == nil {
		return ""
	}
	return s.session.UUID()
}

func (s *Scene) Contact() *flows.Contact        { return s.contact }
func (s *Scene) ContactID() ContactID           { return ContactID(s.contact.ID()) }
func (s *Scene) ContactUUID() flows.ContactUUID { return s.contact.UUID() }
func (s *Scene) UserID() UserID                 { return s.userID }

// Session returns the session for this scene if any
func (s *Scene) Session() *Session { return s.session }

// AttachPreCommitHook adds an item to be handled by the given pre commit hook
func (s *Scene) AttachPreCommitHook(hook SceneCommitHook, item any) {
	s.preCommits[hook] = append(s.preCommits[hook], item)
}

// AttachPostCommitHook adds an item to be handled by the given post commit hook
func (s *Scene) AttachPostCommitHook(hook SceneCommitHook, item any) {
	s.postCommits[hook] = append(s.postCommits[hook], item)
}

// AddEvents runs the given events through the appropriate handlers which in turn attach hooks to the scene
func (s *Scene) AddEvents(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets, events []flows.Event) error {
	for _, e := range events {
		handler, found := eventHandlers[e.Type()]
		if !found {
			return fmt.Errorf("unable to find handler for event type: %s", e.Type())
		}

		if err := handler(ctx, rt, oa, s, e); err != nil {
			return err
		}
	}
	return nil
}

// EventHandler defines a call for handling events that occur in a flow
type EventHandler func(context.Context, *runtime.Runtime, *OrgAssets, *Scene, flows.Event) error

// our registry of event type to internal handlers
var eventHandlers = make(map[string]EventHandler)

// RegisterEventHandler registers the passed in handler as being interested in the passed in type
func RegisterEventHandler(eventType string, handler EventHandler) {
	// it's a bug if we try to register more than one handler for a type
	_, found := eventHandlers[eventType]
	if found {
		panic(fmt.Errorf("duplicate handler being registered for type: %s", eventType))
	}
	eventHandlers[eventType] = handler
}

// SceneCommitHook defines a callback that will accept a certain type of events across session, either before or after committing
type SceneCommitHook interface {
	Order() int
	Apply(context.Context, *runtime.Runtime, *sqlx.Tx, *OrgAssets, map[*Scene][]any) error
}

// ApplyScenePreCommitHooks applies through all the pre commit hooks for the given scenes
func ApplyScenePreCommitHooks(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *OrgAssets, scenes []*Scene) error {
	// gather all our hook events together across our sessions
	byHook := make(map[SceneCommitHook]map[*Scene][]any)
	for _, s := range scenes {
		for hook, args := range s.preCommits {
			byScene, found := byHook[hook]
			if !found {
				byScene = make(map[*Scene][]any, len(scenes))
				byHook[hook] = byScene
			}
			byScene[s] = args
		}
	}

	// get hooks by their declared order
	hookTypes := slices.SortedStableFunc(maps.Keys(byHook), func(h1, h2 SceneCommitHook) int { return cmp.Compare(h1.Order(), h2.Order()) })

	// and apply them in that order
	for _, hook := range hookTypes {
		if err := hook.Apply(ctx, rt, tx, oa, byHook[hook]); err != nil {
			return fmt.Errorf("error applying scene pre commit hook: %T: %w", hook, err)
		}
	}

	return nil
}

// applies through all the post commit hooks for the given scenes in the given transaction
func applyScenePostCommitHooksTx(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *OrgAssets, scenes []*Scene) error {
	// gather all our hook events together across our sessions
	byHook := make(map[SceneCommitHook]map[*Scene][]any)
	for _, s := range scenes {
		for hook, args := range s.postCommits {
			byScene, found := byHook[hook]
			if !found {
				byScene = make(map[*Scene][]any, len(scenes))
				byHook[hook] = byScene
			}
			byScene[s] = args
		}
	}

	// get hooks by their declared order
	hookTypes := slices.SortedStableFunc(maps.Keys(byHook), func(h1, h2 SceneCommitHook) int { return cmp.Compare(h1.Order(), h2.Order()) })

	// and apply them in that order
	for _, hook := range hookTypes {
		if err := hook.Apply(ctx, rt, tx, oa, byHook[hook]); err != nil {
			return fmt.Errorf("error applying scene post commit hook: %T: %w", hook, err)
		}
	}

	return nil
}

// ApplyScenePostCommitHooks applies the post commit hooks for the given scenes
func ApplyScenePostCommitHooks(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets, scenes []*Scene) error {
	if len(scenes) == 0 {
		return nil
	}

	txCTX, cancel := context.WithTimeout(ctx, postCommitTimeout*time.Duration(len(scenes)))
	defer cancel()

	tx, err := rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	err = applyScenePostCommitHooksTx(txCTX, rt, tx, oa, scenes)
	if err == nil {
		err = tx.Commit()
	}

	if err != nil {
		tx.Rollback()

		// we failed with our post commit hooks, try one at a time, logging those errors
		for _, scene := range scenes {
			log := slog.With("contact", scene.ContactUUID(), "session", scene.SessionUUID())

			txCTX, cancel := context.WithTimeout(ctx, postCommitTimeout)
			defer cancel()

			tx, err := rt.DB.BeginTxx(txCTX, nil)
			if err != nil {
				tx.Rollback()
				log.Error("error beginning transaction for retry", "error", err)
				continue
			}

			if err := applyScenePostCommitHooksTx(ctx, rt, tx, oa, []*Scene{scene}); err != nil {
				tx.Rollback()
				log.Error("error applying post commit hook", "error", err)
				continue
			}

			if err := tx.Commit(); err != nil {
				tx.Rollback()
				log.Error("error committing post commit hook", "error", err)
				continue
			}
		}
	}

	return nil
}

// HandleAndCommitEvents takes a set of contacts and events, handles the events and applies any hooks, and commits everything
func HandleAndCommitEvents(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets, userID UserID, contactEvents map[*flows.Contact][]flows.Event) error {
	// create scenes for each contact
	scenes := make([]*Scene, 0, len(contactEvents))
	for contact := range contactEvents {
		scene := NewSceneForContact(contact, userID)
		scenes = append(scenes, scene)
	}

	// begin the transaction for pre-commit hooks
	tx, err := rt.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	// handle the events to create the hooks on each scene
	for _, scene := range scenes {
		if err := scene.AddEvents(ctx, rt, oa, contactEvents[scene.Contact()]); err != nil {
			return fmt.Errorf("error applying events: %w", err)
		}
	}

	// gather all our pre commit events, group them by hook and apply them
	if err := ApplyScenePreCommitHooks(ctx, rt, tx, oa, scenes); err != nil {
		return fmt.Errorf("error applying pre commit hooks: %w", err)
	}

	// commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing pre commit hooks: %w", err)
	}

	// now take care of any post-commit hooks
	if err := ApplyScenePostCommitHooks(ctx, rt, oa, scenes); err != nil {
		return fmt.Errorf("error processing post commit hooks: %w", err)
	}

	return nil
}

// ApplyModifiers modifies contacts by applying modifiers and handling the resultant events
// Note that we don't load the user object from org assets because it's possible that the user isn't part
// of the org, e.g. customer support.
func ApplyModifiers(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets, userID UserID, modifiersByContact map[*flows.Contact][]flows.Modifier) (map[*flows.Contact][]flows.Event, error) {
	// create an environment instance with location support
	env := flows.NewAssetsEnvironment(oa.Env(), oa.SessionAssets())

	eng := goflow.Engine(rt)

	eventsByContact := make(map[*flows.Contact][]flows.Event, len(modifiersByContact))

	// apply the modifiers to get the events for each contact
	for contact, mods := range modifiersByContact {
		events := make([]flows.Event, 0)
		for _, mod := range mods {
			modifiers.Apply(eng, env, oa.SessionAssets(), contact, mod, func(e flows.Event) { events = append(events, e) })
		}
		eventsByContact[contact] = events
	}

	if err := HandleAndCommitEvents(ctx, rt, oa, userID, eventsByContact); err != nil {
		return nil, fmt.Errorf("error commiting events: %w", err)
	}

	return eventsByContact, nil
}

// TypeSprintEnded is a pseudo event that lets add hooks for changes to a contacts current flow or flow history
const TypeSprintEnded string = "sprint_ended"

type SprintEndedEvent struct {
	events.BaseEvent

	Contact *Contact // model contact so we can access current flow
	Resumed bool     // whether this was a resume
}

// NewSprintEndedEvent creates a new sprint ended event
func NewSprintEndedEvent(c *Contact, resumed bool) *SprintEndedEvent {
	return &SprintEndedEvent{
		BaseEvent: events.NewBaseEvent(TypeSprintEnded),
		Contact:   c,
		Resumed:   resumed,
	}
}
