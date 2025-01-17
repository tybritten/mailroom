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
	SessionID   SessionID `json:"session_id,omitempty"`
	WaitResumes bool      `json:"wait_resumes,omitempty"`
}

type ContactFire struct {
	ID        ContactFireID           `db:"id"         json:"-"`
	OrgID     OrgID                   `db:"org_id"     json:"-"`
	ContactID ContactID               `db:"contact_id" json:"contact_id"`
	Type      ContactFireType         `db:"fire_type"  json:"type"`
	Scope     string                  `db:"scope"      json:"scope,omitempty"`
	Extra     JSONB[ContactFireExtra] `db:"extra"      json:"extra,omitempty"`
	FireOn    time.Time               `db:"fire_on"    json:"fire_on"`
}

const sqlSelectDueContactFires = `
  SELECT id, org_id, contact_id, fire_type, scope, extra
    FROM contacts_contactfire
   WHERE fire_on < NOW()
ORDER BY fire_on ASC
   LIMIT 50000`

func LoadDueContactfires(ctx context.Context, rt *runtime.Runtime) (map[OrgID][]*ContactFire, error) {
	rows, err := rt.DB.QueryxContext(ctx, sqlSelectDueContactFires)
	if err != nil {
		return nil, fmt.Errorf("error querying due contact fires: %w", err)
	}
	defer rows.Close()

	// scan and organize by org
	byOrg := make(map[OrgID][]*ContactFire, 50)

	for rows.Next() {
		f := &ContactFire{}
		if err := rows.StructScan(f); err != nil {
			return nil, fmt.Errorf("error scanning contact fire: %w", err)
		}

		byOrg[f.OrgID] = append(byOrg[f.OrgID], f)
	}

	return byOrg, nil
}

func DeleteContactFires(ctx context.Context, rt *runtime.Runtime, ids []ContactFireID) error {
	_, err := rt.DB.ExecContext(ctx, `DELETE FROM contacts_contactfire WHERE id = ANY($1)`, pq.Array(ids))
	if err != nil {
		return fmt.Errorf("error deleting contact fires: %w", err)
	}

	return nil
}
