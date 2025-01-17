package contacts

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

// TypeBulkFire is the type of the task
const TypeBulkFire = "bulk_fire"

func init() {
	tasks.RegisterType(TypeBulkFire, func() tasks.Task { return &BulkFireTask{} })
}

// BulkFireTask is the payload of the task
type BulkFireTask struct {
	Fires []*models.ContactFire `json:"fires"`
}

func (t *BulkFireTask) Type() string {
	return TypeBulkFire
}

// Timeout is the maximum amount of time the task can run for
func (t *BulkFireTask) Timeout() time.Duration {
	return time.Minute * 10
}

func (t *BulkFireTask) WithAssets() models.Refresh {
	return models.RefreshNone
}

// Perform creates the actual task
func (t *BulkFireTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	//timeoutsToResume := make([]models.SessionID, 0, 100)
	//expirationsToResume := make([]models.SessionID, 0, 100)
	sessionsToExit := make([]models.SessionID, 0, 100)

	for _, fire := range t.Fires {
		if fire.Type == models.ContactFireTypeWaitExpiration {
			if fire.Extra.V.WaitResumes {
				// TODO
			} else {
				sessionsToExit = append(sessionsToExit, fire.Extra.V.SessionID)
			}
		} else if fire.Type == models.ContactFireTypeWaitTimeout {
			// TODO
		} else if fire.Type == models.ContactFireTypeCampaign {
			// TODO
		}
	}

	// exit the sessions that can't be resumed
	if err := models.ExitSessions(ctx, rt.DB, sessionsToExit, models.SessionStatusExpired); err != nil {
		return fmt.Errorf("error exiting non-resumable expired sessions: %w", err)
	}

	return nil
}
