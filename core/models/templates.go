package models

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static"
	"github.com/pkg/errors"
)

type Template struct {
	t struct {
		Name         string                 `json:"name"          validate:"required"`
		UUID         assets.TemplateUUID    `json:"uuid"          validate:"required"`
		Translations []*TemplateTranslation `json:"translations"  validate:"dive"`
	}

	translations []assets.TemplateTranslation
}

func (t *Template) UUID() assets.TemplateUUID                  { return t.t.UUID }
func (t *Template) Name() string                               { return t.t.Name }
func (t *Template) Translations() []assets.TemplateTranslation { return t.translations }

func (t *Template) FindTranslation(l i18n.Locale) *TemplateTranslation {
	for _, tt := range t.t.Translations {
		if tt.Locale() == l {
			return tt
		}
	}
	return nil
}

func (t *Template) UnmarshalJSON(d []byte) error {
	if err := json.Unmarshal(d, &t.t); err != nil {
		return err
	}

	t.translations = make([]assets.TemplateTranslation, len(t.t.Translations))
	for i := range t.t.Translations {
		t.translations[i] = t.t.Translations[i]
	}
	return nil
}

type TemplateTranslation struct {
	t struct {
		Channel        *assets.ChannelReference             `json:"channel"`
		Namespace      string                               `json:"namespace"`
		Locale         i18n.Locale                          `json:"locale"`
		ExternalLocale string                               `json:"external_locale"`
		Components     map[string]*static.TemplateComponent `json:"components"`
	}

	components map[string]assets.TemplateComponent
}

func (t *TemplateTranslation) Channel() *assets.ChannelReference { return t.t.Channel }
func (t *TemplateTranslation) Namespace() string                 { return t.t.Namespace }
func (t *TemplateTranslation) Locale() i18n.Locale               { return t.t.Locale }
func (t *TemplateTranslation) ExternalLocale() string            { return t.t.ExternalLocale }

func (t *TemplateTranslation) Components() map[string]assets.TemplateComponent { return t.components }

func (t *TemplateTranslation) UnmarshalJSON(d []byte) error {
	if err := json.Unmarshal(d, &t.t); err != nil {
		return err
	}

	t.components = make(map[string]assets.TemplateComponent, len(t.t.Components))
	for comp, compData := range t.t.Components {
		t.components[comp] = compData
	}
	return nil
}

// loads the templates for the passed in org
func loadTemplates(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Template, error) {
	rows, err := db.QueryContext(ctx, sqlSelectTemplatesByOrg, orgID)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying templates for org: %d", orgID)
	}
	return ScanJSONRows(rows, func() assets.Template { return &Template{} })
}

const sqlSelectTemplatesByOrg = `
SELECT ROW_TO_JSON(r) FROM (
     SELECT t.uuid, t.name, (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(tr))) FROM (
         SELECT tr.namespace, tr.locale, tr.external_locale, tr.components, JSON_BUILD_OBJECT('uuid', c.uuid, 'name', c.name) as channel
           FROM templates_templatetranslation tr
           JOIN channels_channel c ON tr.channel_id = c.id
          WHERE tr.is_active = TRUE AND tr.status = 'A' AND tr.template_id = t.id AND c.is_active = TRUE
         ) tr) as translations
       FROM templates_template t
      WHERE org_id = $1 
   ORDER BY name ASC
) r;`
