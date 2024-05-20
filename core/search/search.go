package search

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/nyaruka/gocommon/elastic"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/contactql/es"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	eslegacy "github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// AssetMapper maps resolved assets in queries to how we identify them in ES which in the case
// of flows and groups is their ids. We can do this by just type cracking them to their models.
type AssetMapper struct{}

func (m *AssetMapper) Flow(f assets.Flow) int64 {
	return int64(f.(*models.Flow).ID())
}

func (m *AssetMapper) Group(g assets.Group) int64 {
	return int64(g.(*models.Group).ID())
}

var assetMapper = &AssetMapper{}

// BuildElasticQuery turns the passed in contact ql query into an elastic query
func BuildElasticQuery(oa *models.OrgAssets, group *models.Group, status models.ContactStatus, excludeIDs []models.ContactID, query *contactql.ContactQuery) elastic.Query {
	// filter by org and active contacts
	must := []elastic.Query{
		elastic.Term("org_id", oa.OrgID()),
		elastic.Term("is_active", true),
	}

	// and group if present
	if group != nil {
		must = append(must, elastic.Term("group_ids", group.ID()))
	}

	// and status if present
	if status != models.NilContactStatus {
		must = append(must, elastic.Term("status", status))
	}

	// and by user query if present
	if query != nil {
		must = append(must, es.ToElasticQuery(oa.Env(), assetMapper, query))
	}

	not := []elastic.Query{}

	// exclude ids if present
	if len(excludeIDs) > 0 {
		ids := make([]string, len(excludeIDs))
		for i := range excludeIDs {
			ids[i] = fmt.Sprintf("%d", excludeIDs[i])
		}
		not = append(not, elastic.Ids(ids...))
	}

	return elastic.Bool(must, not)
}

// GetContactTotal returns the total count of matching contacts for the given query
func GetContactTotal(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, query string) (*contactql.ContactQuery, int64, error) {
	env := oa.Env()
	var parsed *contactql.ContactQuery
	var err error

	if rt.ES == nil {
		return nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	if query != "" {
		parsed, err = contactql.ParseQuery(env, query, oa.SessionAssets())
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	routing := strconv.FormatInt(int64(oa.OrgID()), 10)
	eq := BuildElasticQuery(oa, group, models.NilContactStatus, nil, parsed)
	src := map[string]any{"query": eq}

	count, err := rt.ES.Count(rt.Config.ElasticContactsIndex).Routing(routing).BodyJson(src).Do(ctx)
	if err != nil {
		// Get *elastic.Error which contains additional information
		ee, ok := err.(*eslegacy.Error)
		if !ok {
			return nil, 0, errors.Wrap(err, "error performing query")
		}

		return nil, 0, errors.Wrapf(err, "error performing query: %s", ee.Details.Reason)
	}

	return parsed, count, nil
}

// GetContactIDsForQueryPage returns a page of contact ids for the given query and sort
func GetContactIDsForQueryPage(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, excludeIDs []models.ContactID, query string, sort string, offset int, pageSize int) (*contactql.ContactQuery, []models.ContactID, int64, error) {
	env := oa.Env()
	index := rt.Config.ElasticContactsIndex
	start := time.Now()
	var parsed *contactql.ContactQuery
	var err error

	if rt.ES == nil {
		return nil, nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	if query != "" {
		parsed, err = contactql.ParseQuery(env, query, oa.SessionAssets())
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	routing := strconv.FormatInt(int64(oa.OrgID()), 10)
	eq := BuildElasticQuery(oa, group, models.NilContactStatus, excludeIDs, parsed)

	fieldSort, err := es.ToElasticSort(sort, oa.SessionAssets())
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error parsing sort")
	}

	src := map[string]any{
		"_source":          false,
		"query":            eq,
		"sort":             []any{fieldSort},
		"from":             offset,
		"size":             pageSize,
		"track_total_hits": true,
	}

	s := rt.ES.Search(index).Routing(routing).Source(string(jsonx.MustMarshal(src)))

	results, err := s.Do(ctx)
	if err != nil {
		// Get *elastic.Error which contains additional information
		ee, ok := err.(*eslegacy.Error)
		if !ok {
			return nil, nil, 0, errors.Wrapf(err, "error performing query")
		}

		return nil, nil, 0, errors.Wrapf(err, "error performing query: %s", ee.Details.Reason)
	}

	ids := make([]models.ContactID, 0, pageSize)
	ids, err = appendIDsFromHits(ids, results.Hits.Hits)
	if err != nil {
		return nil, nil, 0, err
	}

	slog.Debug("paged contact query complete", "org_id", oa.OrgID(), "query", query, "elapsed", time.Since(start), "page_count", len(ids), "total_count", results.Hits.TotalHits.Value)

	return parsed, ids, results.Hits.TotalHits.Value, nil
}

// GetContactIDsForQuery returns up to limit the contact ids that match the given query, sorted by id. Limit of -1 means return all.
func GetContactIDsForQuery(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, status models.ContactStatus, query string, limit int) ([]models.ContactID, error) {
	env := oa.Env()
	index := rt.Config.ElasticContactsIndex
	start := time.Now()
	var parsed *contactql.ContactQuery
	var err error

	if rt.ES == nil {
		return nil, errors.Errorf("no elastic client available, check your configuration")
	}

	// turn into elastic query
	if query != "" {
		parsed, err = contactql.ParseQuery(env, query, oa.SessionAssets())
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	routing := strconv.FormatInt(int64(oa.OrgID()), 10)
	eq := BuildElasticQuery(oa, group, status, nil, parsed)
	sort := elastic.SortBy("id", true)

	ids := make([]models.ContactID, 0, 100)

	// if limit provided that can be done with regular search, do that
	if limit >= 0 && limit <= 10000 {
		src := map[string]any{
			"_source": false,
			"query":   eq,
			"sort":    []any{sort},
			"from":    0,
			"size":    limit,
		}

		results, err := rt.ES.Search(index).Routing(routing).Source(string(jsonx.MustMarshal(src))).Do(ctx)
		if err != nil {
			return nil, err
		}
		return appendIDsFromHits(ids, results.Hits.Hits)
	}

	// for larger limits, use scroll service
	// note that this is no longer recommended, see https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html
	src := map[string]any{
		"_source": false,
		"query":   eq,
		"sort":    []any{sort},
	}

	scroll := rt.ES.Scroll(index).Routing(routing).KeepAlive("15m").Body(src).Size(10000)
	for {
		results, err := scroll.Do(ctx)
		if err == io.EOF {
			slog.Debug("contact query complete", "org_id", oa.OrgID(), "query", query, "elapsed", time.Since(start), "match_count", len(ids))

			return ids, nil
		}
		if err != nil {
			return nil, errors.Wrapf(err, "error scrolling through results for search: %s", query)
		}

		ids, err = appendIDsFromHits(ids, results.Hits.Hits)
		if err != nil {
			return nil, err
		}
	}
}

// utility to convert search hits to contact IDs and append them to the given slice
func appendIDsFromHits(ids []models.ContactID, hits []*eslegacy.SearchHit) ([]models.ContactID, error) {
	for _, hit := range hits {
		id, err := strconv.Atoi(hit.Id)
		if err != nil {
			return nil, errors.Wrapf(err, "unexpected non-integer contact id: %s", hit.Id)
		}

		ids = append(ids, models.ContactID(id))
	}
	return ids, nil
}
