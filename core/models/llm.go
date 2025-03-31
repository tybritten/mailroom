package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/test/services"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
)

// LLMID is our type for LLM IDs
type LLMID int

// NilLLMID is nil value for LLM IDs
const NilLLMID = LLMID(0)

var registeredLLMServices = map[string]func(*LLM) (flows.LLMService, error){}

// Register a LLM service factory with the engine
func init() {
	RegisterLLMService("test", func(llm *LLM) (flows.LLMService, error) {
		return services.NewLLM(), nil
	})

	goflow.RegisterLLMServiceFactory(llmServiceFactory)
}

// RegisterLLMService registers a LLM service for the given type code
func RegisterLLMService(typ string, fn func(*LLM) (flows.LLMService, error)) {
	registeredLLMServices[typ] = fn
}

func llmServiceFactory(rt *runtime.Runtime) engine.LLMServiceFactory {
	return func(llm *flows.LLM) (flows.LLMService, error) {
		return llm.Asset().(*LLM).AsService()
	}
}

// LLM is our type for a large language model
type LLM struct {
	ID_     LLMID          `json:"id"`
	UUID_   assets.LLMUUID `json:"uuid"`
	Type_   string         `json:"llm_type"`
	Name_   string         `json:"name"`
	Config_ Config         `json:"config"`
}

func (l *LLM) ID() LLMID            { return l.ID_ }
func (l *LLM) UUID() assets.LLMUUID { return l.UUID_ }
func (l *LLM) Name() string         { return l.Name_ }
func (l *LLM) Type() string         { return l.Type_ }
func (l *LLM) Config() Config       { return l.Config_ }

func (l *LLM) AsService() (flows.LLMService, error) {
	fn := registeredLLMServices[l.Type()]
	if fn == nil {
		return nil, fmt.Errorf("unknown type '%s' for LLM: %s", l.Type(), l.UUID())
	}
	return fn(l)
}

// loads the LLMs for the passed in org
func loadLLMs(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.LLM, error) {
	rows, err := db.QueryContext(ctx, sqlSelectLLMs, orgID)
	if err != nil {
		return nil, fmt.Errorf("error querying LLMs for org: %d: %w", orgID, err)
	}

	return ScanJSONRows(rows, func() assets.LLM { return &LLM{} })
}

const sqlSelectLLMs = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT l.id, l.uuid, l.name, l.llm_type, l.config
        FROM ai_llm l
       WHERE l.org_id = $1 AND l.is_active
    ORDER BY l.created_on ASC
) r;`

func (i *LLMID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i LLMID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *LLMID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i LLMID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
