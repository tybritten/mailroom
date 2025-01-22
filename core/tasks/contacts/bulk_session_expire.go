package contacts

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/ctasks"
	"github.com/nyaruka/mailroom/runtime"
)

// TypeBulkSessionExpire is the type of the task
const TypeBulkSessionExpire = "bulk_session_expire"

func init() {
	tasks.RegisterType(TypeBulkSessionExpire, func() tasks.Task { return &BulkSessionExpireTask{} })
}

type Expiration struct {
	ContactID  models.ContactID `json:"contact_id"`
	SessionID  models.SessionID `json:"session_id"`
	ModifiedOn time.Time        `json:"modified_on"`
}

// BulkSessionExpireTask is the payload of the task
type BulkSessionExpireTask struct {
	Expirations []*Expiration `json:"expirations"`
}

func (t *BulkSessionExpireTask) Type() string {
	return TypeBulkSessionExpire
}

// Timeout is the maximum amount of time the task can run for
func (t *BulkSessionExpireTask) Timeout() time.Duration {
	return time.Hour
}

func (t *BulkSessionExpireTask) WithAssets() models.Refresh {
	return models.RefreshNone
}

// Perform creates the actual task
func (t *BulkSessionExpireTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	rc := rt.RP.Get()
	defer rc.Close()

	for _, exp := range t.Expirations {
		err := handler.QueueTask(rc, oa.OrgID(), exp.ContactID, ctasks.NewWaitExpiration(exp.SessionID, exp.ModifiedOn))
		if err != nil {
			return fmt.Errorf("error queuing handle task for expiration on session #%d: %w", exp.SessionID, err)
		}
	}

	return nil
}
