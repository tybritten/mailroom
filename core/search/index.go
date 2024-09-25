package search

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/operationtype"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// DeindexContactsByID de-indexes the contacts with the given IDs from Elastic
func DeindexContactsByID(ctx context.Context, rt *runtime.Runtime, contactIDs []models.ContactID) (int, error) {
	rows, err := rt.DB.QueryContext(ctx, `SELECT id, org_id, modified_on FROM contacts_contact WHERE id = ANY($1) AND NOT is_active`, pq.Array(contactIDs))
	if err != nil {
		return 0, fmt.Errorf("error querying deleted contacts to deindex: %w", err)
	}
	defer rows.Close()

	cmds := &bytes.Buffer{}

	for rows.Next() {
		var id models.ContactID
		var orgID models.OrgID
		var modifiedOn time.Time

		if err := rows.Scan(&id, &orgID, &modifiedOn); err != nil {
			return 0, fmt.Errorf("error scanning deleted contact to deindex: %w", err)
		}

		cmds.Write(jsonx.MustMarshal(map[string]any{
			"delete": map[string]any{
				"_id":          id,
				"version":      modifiedOn.UnixNano(),
				"version_type": "external",
				"routing":      orgID.String(),
			}},
		))
		cmds.WriteString("\n")
	}

	if cmds.Len() == 0 {
		return 0, nil
	}

	fmt.Println(cmds.String())

	resp, err := rt.ES.Bulk().Index(rt.Config.ElasticContactsIndex).Raw(bytes.NewReader(cmds.Bytes())).Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("error deindexing deleted contacts from elastic: %w", err)
	}

	deleted := 0
	for _, r := range resp.Items {
		if r[operationtype.Delete].Status == 200 {
			deleted++
		}
	}

	return deleted, nil
}

// DeindexContactsByOrg de-indexes all contacts in the given org from Elastic
func DeindexContactsByOrg(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	src := map[string]any{
		"query": map[string]any{"match_all": map[string]any{}},
	}

	_, err := rt.ES.DeleteByQuery(rt.Config.ElasticContactsIndex).Routing(orgID.String()).Raw(bytes.NewReader(jsonx.MustMarshal(src))).Do(ctx)
	if err != nil {
		return fmt.Errorf("error deindexing contacts in org #%d from elastic: %w", orgID, err)
	}

	return nil
}
