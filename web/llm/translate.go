package llm

import (
	"context"
	"fmt"
	"net/http"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/llm/translate", web.RequireAuthToken(web.JSONPayload(handleTranslate)))
}

// Performs translation using an LLM.
//
//	{
//	  "org_id": 1,
//	  "llm_id": 1234,
//	  "from_language": "eng",
//	  "to_language": "spa",
//	  "text": "Hello world"
//	}
type translateRequest struct {
	OrgID        models.OrgID  `json:"org_id"        validate:"required"`
	LLMID        models.LLMID  `json:"llm_id"        validate:"required"`
	FromLanguage i18n.Language `json:"from_language" validate:"required"`
	ToLanguage   i18n.Language `json:"to_language"   validate:"required"`
	Text         string        `json:"text"          validate:"required"`
}

//	{
//	  "text": "Hola mundo"
//	}
type translateResponse struct {
	Text string `json:"text"`
}

func handleTranslate(ctx context.Context, rt *runtime.Runtime, r *translateRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, fmt.Errorf("error loading org assets: %w", err)
	}

	llm := oa.LLMByID(r.LLMID)
	if llm == nil {
		return nil, 0, fmt.Errorf("no such LLM with ID %d", r.LLMID)
	}

	llmSvc, err := llm.AsService(oa.SessionAssets().LLMs().Get(llm.UUID()))
	if err != nil {
		return nil, 0, fmt.Errorf("error creating LLM service: %w", err)
	}

	var instructions string
	if r.FromLanguage != "und" && r.FromLanguage != "mul" {
		instructions = fmt.Sprintf("Translate the given text from the language with the ISO code %s to the language with the ISO code %s. ", r.FromLanguage, r.ToLanguage)
	} else {
		instructions = fmt.Sprintf("Translate the given text to the language with the ISO code %s. ", r.ToLanguage)
	}
	instructions += "The @ indicates a variable expression and should be left untranslated. Only return the translated text."

	output, err := llmSvc.Response(ctx, oa.Env(), instructions, r.Text)
	if err != nil {
		return nil, 0, fmt.Errorf("error calling LLM service: %w", err)
	}

	return translateResponse{Text: output}, http.StatusOK, nil
}
