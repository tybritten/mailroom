package contact

import (
	"context"
	"fmt"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"golang.org/x/exp/maps"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/urns", web.RequireAuthToken(web.JSONPayload(handleURNs)))
}

// Request to validate a set of URNs and determine ownership.
//
//	{
//	  "org_id": 1,
//	  "urns": ["tel:+593979123456", "webchat:123456", "line:1234567890"]
//	}
//
//	{
//	  "tel:+593979123456": 35657,
//	  "webchat:123456": "invalid path component"
//	  "line:1234567890": null
//	}
type urnsRequest struct {
	OrgID models.OrgID `json:"org_id"   validate:"required"`
	URNs  []urns.URN   `json:"urns"  validate:"required"`
}

// handles a request to create the given contact
func handleURNs(ctx context.Context, rt *runtime.Runtime, r *urnsRequest) (any, int, error) {
	urnsToLookup := make(map[urns.URN]urns.URN, len(r.URNs)) // normalized to original form of valid URNs
	result := make(map[urns.URN]any, len(r.URNs))

	for _, urn := range r.URNs {
		norm := urn.Normalize()
		if err := norm.Validate(); err != nil {
			result[urn] = err.Error()
		} else {
			urnsToLookup[norm] = urn
		}
	}

	owners, err := models.GetContactIDsFromURNs(ctx, rt.DB, r.OrgID, maps.Keys(urnsToLookup))
	if err != nil {
		return nil, 0, fmt.Errorf("error getting URN owners: %w", err)
	}

	for nurn, owner := range owners {
		orig := urnsToLookup[nurn]
		result[orig] = owner
	}

	return result, http.StatusOK, nil
}
