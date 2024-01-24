package models

import (
	"context"
	"database/sql"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static"
	"github.com/pkg/errors"
)

type Template struct {
	Name_         string                 `json:"name"          validate:"required"`
	UUID_         assets.TemplateUUID    `json:"uuid"          validate:"required"`
	Translations_ []*TemplateTranslation `json:"translations"  validate:"dive"`
}

func (t *Template) Name() string              { return t.Name_ }
func (t *Template) UUID() assets.TemplateUUID { return t.UUID_ }
func (t *Template) Translations() []assets.TemplateTranslation {
	trs := make([]assets.TemplateTranslation, len(t.Translations_))
	for i := range trs {
		trs[i] = t.Translations_[i]
	}
	return trs
}

func (t *Template) FindTranslation(l i18n.Locale) *TemplateTranslation {
	for _, tt := range t.Translations_ {
		if tt.Locale() == l {
			return tt
		}
	}
	return nil
}

type TemplateTranslation struct {
	Channel_        *assets.ChannelReference `json:"channel"`
	Namespace_      string                   `json:"namespace"`
	Locale_         i18n.Locale              `json:"locale"`
	ExternalLocale_ string                   `json:"external_locale"`
	Content_        string                   `json:"content"`

	RawParams_ map[string][]static.TemplateParam `json:"params"`
	Params_    map[string][]assets.TemplateParam
}

func (t *TemplateTranslation) Channel() *assets.ChannelReference { return t.Channel_ }
func (t *TemplateTranslation) Namespace() string                 { return t.Namespace_ }
func (t *TemplateTranslation) Locale() i18n.Locale               { return t.Locale_ }
func (t *TemplateTranslation) ExternalLocale() string            { return t.ExternalLocale_ }
func (t *TemplateTranslation) Content() string                   { return t.Content_ }
func (t *TemplateTranslation) Params() map[string][]assets.TemplateParam {
	return t.Params_
}

// loads the templates for the passed in org
func loadTemplates(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Template, error) {
	rows, err := db.QueryContext(ctx, sqlSelectTemplatesByOrg, orgID)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying templates for org: %d", orgID)
	}
	defer rows.Close()

	templates := make([]assets.Template, 0, 10)
	for rows.Next() {
		template := &Template{}
		err = dbutil.ScanJSON(rows, &template)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning template row")
		}

		for _, tt := range template.Translations_ {

			prs := make(map[string][]assets.TemplateParam)

			for k, v := range tt.RawParams_ {
				tprs := make([]assets.TemplateParam, len(v))
				for i := range v {
					tprs[i] = assets.TemplateParam(&v[i])
				}
				prs[k] = tprs
			}

			tt.Params_ = prs
		}

		templateAsset := assets.Template(template)

		templates = append(templates, templateAsset)
	}
	return templates, nil
}

const sqlSelectTemplatesByOrg = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	t.name as name, 
	t.uuid as uuid,
	(SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(tr))) FROM (
		SELECT
			tr.namespace as namespace,
			tr.locale as locale,
			tr.external_locale as external_locale,
			tr.content as content,
			tr.params as params,
			JSON_BUILD_OBJECT('uuid', c.uuid, 'name', c.name) as channel
		FROM
			templates_templatetranslation tr
			JOIN channels_channel c ON tr.channel_id = c.id
		WHERE 
			tr.is_active = TRUE AND
			tr.status = 'A' AND
			tr.template_id = t.id AND
			c.is_active = TRUE
	) tr) as translations
FROM 
	templates_template t
WHERE 
	org_id = $1 
ORDER BY 
	name ASC
) r;
`
