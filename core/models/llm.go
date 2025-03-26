package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/null/v3"
)

// LLMID is our type for LLM IDs
type LLMID int

// NilLLMID is nil value for LLM IDs
const NilLLMID = LLMID(0)

// LLM type constants
const (
	LLMTypeOpenAI = "openai"
)

// LLM config key constants
const (
	// OpenAI config options
	OpenAIConfigAPIKey = "api_key"
	OpenAIConfigModel  = "model"
)

// Register a LLM service factory with the engine
func init() {
	// goflow.RegisterLLMServiceFactory(llmServiceFactory)
}

// func llmServiceFactory(rt *runtime.Runtime) engine.LLMServiceFactory {
// 	return func(llm *flows.LLM) (flows.LLMService, error) {
// 		return llm.Asset().(*LLM).AsService(rt.Config, llm)
// 	}
// }

// LLM is our type for a large language model
type LLM struct {
	ID_     LLMID             `json:"id"`
	UUID_   assets.LLMUUID    `json:"uuid"`
	Type_   string            `json:"llm_type"`
	Name_   string            `json:"name"`
	Config_ map[string]string `json:"config"`
}

// ID returns the ID
func (l *LLM) ID() LLMID { return l.ID_ }

// UUID returns our UUID
func (l *LLM) UUID() assets.LLMUUID { return l.UUID_ }

// Name return our Name
func (l *LLM) Name() string { return l.Name_ }

// Type returns the type
func (l *LLM) Type() string { return l.Type_ }

// AsService builds the corresponding LLMService for the passed in LLM
// func (l *LLM) AsService(cfg *runtime.Config, llm *flows.LLM) (flows.LLMService, error) {
//   	TODO
// }

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
      SELECT l.id, l.uuid, l.name, l.llm_type, l.config,
        FROM ai_llm l
       WHERE l.org_id = $1 AND l.is_active = TRUE
    ORDER BY l.created_on ASC
) r;`

func (i *LLMID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i LLMID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *LLMID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i LLMID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
