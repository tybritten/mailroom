package models

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/mailroom/runtime"
)

type ContactFireID int64
type ContactFireType string

const (
	ContactFireTypeWaitExpiration ContactFireType = "E"
	ContactFireTypeWaitTimeout    ContactFireType = "T"
	ContactFireTypeCampaign       ContactFireType = "C"
)

type ContactFireExtra struct {
	SessionID         SessionID `json:"session_id,omitempty"`
	SessionModifiedOn time.Time `json:"session_modified_on,omitempty"`
	CallID            CallID    `json:"call_id,omitempty"`
}

type ContactFire struct {
	ID        ContactFireID           `db:"id"`
	OrgID     OrgID                   `db:"org_id"`
	ContactID ContactID               `db:"contact_id"`
	Type      ContactFireType         `db:"fire_type"`
	Scope     string                  `db:"scope"`
	Extra     JSONB[ContactFireExtra] `db:"extra"`
	FireOn    time.Time               `db:"fire_on"`
}

const sqlSelectDueContactFires = `
  SELECT id, org_id, contact_id, fire_type, scope, extra
    FROM contacts_contactfire
   WHERE fire_on < NOW()
ORDER BY fire_on ASC
   LIMIT $1`

// LoadDueContactfires returns up to 10,000 contact fires that are due to be fired.
func LoadDueContactfires(ctx context.Context, rt *runtime.Runtime, limit int) ([]*ContactFire, error) {
	rows, err := rt.DB.QueryxContext(ctx, sqlSelectDueContactFires, limit)
	if err != nil {
		return nil, fmt.Errorf("error querying due contact fires: %w", err)
	}
	defer rows.Close()

	fires := make([]*ContactFire, 0, 50)

	for rows.Next() {
		f := &ContactFire{}
		if err := rows.StructScan(f); err != nil {
			return nil, fmt.Errorf("error scanning contact fire: %w", err)
		}
		fires = append(fires, f)
	}

	return fires, nil
}

func DeleteContactFires(ctx context.Context, rt *runtime.Runtime, fires []*ContactFire) error {
	ids := make([]ContactFireID, 0, len(fires))
	for _, f := range fires {
		ids = append(ids, f.ID)
	}

	_, err := rt.DB.ExecContext(ctx, `DELETE FROM contacts_contactfire WHERE id = ANY($1)`, pq.Array(ids))
	if err != nil {
		return fmt.Errorf("error deleting contact fires: %w", err)
	}

	return nil
}

func ClearSessionContactFires(ctx context.Context, rt *runtime.Runtime, s *Session) error {
	_, err := rt.DB.ExecContext(ctx, `DELETE FROM contacts_contactfire WHERE contact_id = $1 AND fire_type IN ('E', 'T')`, s.ContactID())
	if err != nil {
		return fmt.Errorf("error deleting session wait/timeout contact fires for contact #%d: %w", s.ContactID(), err)
	}
	return nil
}

func CreateSessionContactFires(ctx context.Context, s *Session, expiresOn *time.Time, timeoutOn *time.Time) []*ContactFire {
	fs := make([]*ContactFire, 0, 2)

	if expiresOn != nil {
		fs = append(fs, &ContactFire{
			OrgID:     s.OrgID(),
			ContactID: s.ContactID(),
			Type:      ContactFireTypeWaitExpiration,
			Extra:     JSONB[ContactFireExtra]{ContactFireExtra{SessionID: s.ID(), SessionModifiedOn: s.ModifiedOn()}},
			FireOn:    *expiresOn,
		})
	}
	if timeoutOn != nil {
		fs = append(fs, &ContactFire{
			OrgID:     s.OrgID(),
			ContactID: s.ContactID(),
			Type:      ContactFireTypeWaitTimeout,
			Extra:     JSONB[ContactFireExtra]{ContactFireExtra{SessionID: s.ID(), SessionModifiedOn: s.ModifiedOn()}},
			FireOn:    *expiresOn,
		})
	}

	return fs
}

var sqlInsertContactFires = `
INSERT INTO contacts_contactfire( org_id,  contact_id,  fire_type, scope,  extra,  fire_on)
                          VALUES(:org_id, :contact_id, :fire_type,    '', :extra, :fire_on)`

func InsertContactFires(ctx context.Context, db DBorTx, fs []*ContactFire) error {
	return BulkQuery(ctx, "inserted contact fires", db, sqlInsertContactFires, fs)
}
