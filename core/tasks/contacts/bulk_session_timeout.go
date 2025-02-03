package contacts

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/ctasks"
	"github.com/nyaruka/mailroom/runtime"
)

// TypeBulkSessionTimeout is the type of the task
const TypeBulkSessionTimeout = "bulk_session_timeout"

func init() {
	tasks.RegisterType(TypeBulkSessionTimeout, func() tasks.Task { return &BulkSessionTimeoutTask{} })
}

type Timeout struct {
	ContactID   models.ContactID  `json:"contact_id"`
	SessionUUID flows.SessionUUID `json:"session_uuid"`
	SprintUUID  flows.SprintUUID  `json:"sprint_uuid"`

	// deprecated
	SessionID  models.SessionID `json:"session_id"`
	ModifiedOn time.Time        `json:"modified_on"`
}

// BulkSessionTimeoutTask is the payload of the task
type BulkSessionTimeoutTask struct {
	Timeouts []*Timeout `json:"timeouts"`
}

func (t *BulkSessionTimeoutTask) Type() string {
	return TypeBulkSessionTimeout
}

// Timeout is the maximum amount of time the task can run for
func (t *BulkSessionTimeoutTask) Timeout() time.Duration {
	return time.Hour
}

func (t *BulkSessionTimeoutTask) WithAssets() models.Refresh {
	return models.RefreshNone
}

// Perform creates the actual task
func (t *BulkSessionTimeoutTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	rc := rt.RP.Get()
	defer rc.Close()

	for _, e := range t.Timeouts {
		err := handler.QueueTask(rc, oa.OrgID(), e.ContactID, &ctasks.WaitTimeoutTask{SessionUUID: e.SessionUUID, SprintUUID: e.SprintUUID, SessionID: e.SessionID, ModifiedOn: e.ModifiedOn})
		if err != nil {
			return fmt.Errorf("error queuing handle task for expiration on session %s: %w", e.SessionUUID, err)
		}
	}

	return nil
}
