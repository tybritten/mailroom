package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/export_preview", web.RequireAuthToken(web.JSONPayload(handleExportPreview)))
}

// Generates a preview of which contacts will be included in an export.
//
//	{
//	  "org_id": 1,
//	  "group_id": 45,
//	  "query": "age < 65"
//	}
//
//	{
//	  "total": 567
//	}
type previewRequest struct {
	OrgID   models.OrgID   `json:"org_id"   validate:"required"`
	GroupID models.GroupID `json:"group_id" validate:"required"`
	Query   string         `json:"query"`
}

type previewResponse struct {
	Total int `json:"total"`
}

func handleExportPreview(ctx context.Context, rt *runtime.Runtime, r *previewRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to load org assets")
	}

	group := oa.GroupByID(r.GroupID)
	if group == nil {
		return errors.New("no such group"), http.StatusBadRequest, nil
	}

	// if there's no query, just lookup group count from db
	if r.Query == "" {
		count, err := models.GetGroupContactCount(ctx, rt.DB.DB, group.ID())
		if err != nil {
			return nil, 0, errors.Wrap(err, "error querying group count")
		}
		return &previewResponse{Total: count}, http.StatusOK, nil
	}

	_, total, err := search.GetContactTotal(ctx, rt, oa, group, r.Query)
	if err != nil {
		isQueryError, qerr := contactql.IsQueryError(err)
		if isQueryError {
			return qerr, http.StatusBadRequest, nil
		}
		return nil, 0, errors.Wrap(err, "error querying preview")
	}

	return &previewResponse{Total: int(total)}, http.StatusOK, nil
}
