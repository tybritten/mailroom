package runner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// ResumeFlow resumes the passed in session using the passed in session
func ResumeFlow(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, session *models.Session, contact *models.Contact, resume flows.Resume, hook models.SessionCommitHook) (*models.Session, error) {
	start := time.Now()
	sa := oa.SessionAssets()

	// does the flow this session is part of still exist?
	_, err := oa.FlowByID(session.CurrentFlowID())
	if err != nil {
		// if this flow just isn't available anymore, log this error
		if err == models.ErrNotFound {
			slog.Error("unable to find flow for resume", "contact_uuid", session.Contact().UUID(), "session_uuid", session.UUID(), "flow_id", session.CurrentFlowID())
			return nil, models.ExitSessions(ctx, rt.DB, []flows.SessionUUID{session.UUID()}, models.SessionStatusFailed)
		}
		return nil, fmt.Errorf("error loading session flow: %d: %w", session.CurrentFlowID(), err)
	}

	// build our flow session
	fs, err := session.FlowSession(ctx, rt, sa, oa.Env())
	if err != nil {
		return nil, fmt.Errorf("unable to create session from output: %w", err)
	}

	// resume our session
	sprint, err := fs.Resume(ctx, resume)

	// had a problem resuming our flow? bail
	if err != nil {
		return nil, fmt.Errorf("error resuming flow: %w", err)
	}

	// write our updated session, applying any events in the process
	txCTX, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()

	tx, err := rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	// write our updated session and runs
	if err := session.Update(txCTX, rt, tx, oa, fs, sprint, contact, hook); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error updating session for resume: %w", err)
	}

	// commit at once
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error committing resumption of flow: %w", err)
	}

	// now take care of any post-commit hooks
	txCTX, cancel = context.WithTimeout(ctx, postCommitTimeout)
	defer cancel()

	tx, err = rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction for post commit hooks: %w", err)
	}

	if err = models.ApplyEventPostCommitHooks(txCTX, rt, tx, oa, []*models.Scene{session.Scene()}); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error applying post commit hooks on resume: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing post commit hook changes on resume: %w", err)
	}

	slog.Debug("resumed session", "contact", resume.Contact().UUID(), "session", session.UUID(), "resume_type", resume.Type(), "elapsed", time.Since(start))

	return session, nil
}
