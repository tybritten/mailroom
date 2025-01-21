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
	rc := rt.RP.Get()
	defer rc.Close()

	for _, fire := range t.Fires {
		if fire.Type == models.ContactFireTypeWaitExpiration {
<<<<<<< Updated upstream
			err := handler.QueueTask(rc, oa.OrgID(), fire.ContactID, ctasks.NewWaitExpiration(fire.Extra.V.SessionID, exp.ModifiedOn))
			if err != nil {
				return fmt.Errorf("error queuing handle task for expiration on session #%d: %w", exp.SessionID, err)
=======
			err := handler.QueueTask(rc, oa.OrgID(), fire.ContactID, ctasks.NewWaitExpiration(fire.Extra.V.SessionID, fire.Extra.V.SessionModifiedOn))
			if err != nil {
				return fmt.Errorf("error queuing handle task for expiration on session #%d: %w", fire.Extra.V.SessionID, err)
>>>>>>> Stashed changes
			}
		} else if fire.Type == models.ContactFireTypeWaitTimeout {
			// TODO
		} else if fire.Type == models.ContactFireTypeCampaign {
			// TODO
		}
	}

	return nil
}
