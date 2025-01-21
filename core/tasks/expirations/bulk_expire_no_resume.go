package expirations

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

// TypeBulkExpireNoResume is the type of the task
const TypeBulkExpireNoResume = "bulk_expire_no_resume"

func init() {
	tasks.RegisterType(TypeBulkExpireNoResume, func() tasks.Task { return &BulkExpireNoResumeTask{} })
}

// BulkExpireNoResumeTask is the payload of the task
type BulkExpireNoResumeTask struct {
	Expirations []*ExpiredWait `json:"expirations"`
}

func (t *BulkExpireNoResumeTask) Type() string {
	return TypeBulkExpireNoResume
}

// Timeout is the maximum amount of time the task can run for
func (t *BulkExpireNoResumeTask) Timeout() time.Duration {
	return time.Hour
}

func (t *BulkExpireNoResumeTask) WithAssets() models.Refresh {
	return models.RefreshNone
}

// Perform creates the actual task
func (t *BulkExpireNoResumeTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	rc := rt.RP.Get()
	defer rc.Close()

	ids := make([]models.SessionID, 0, len(t.Expirations))
	for _, exp := range t.Expirations {
		ids = append(ids, exp.SessionID)
	}

	// TODO should do something smarter here to ensure we're not exiting sessions that have been modified since

	if err := models.ExitSessions(ctx, rt.DB, ids, models.SessionStatusExpired); err != nil {
		return fmt.Errorf("error exiting expired sessions: %w", err)
	}

	return nil
}
