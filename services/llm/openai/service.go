package openai

import (
	"context"
	"fmt"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/responses"
	"github.com/openai/openai-go/shared"
)

// an LLM service implementation for OpenAI
type service struct {
	client openai.Client
	llm    *flows.LLM
	model  string
}

// NewService creates a new classification service
func NewService(llm *flows.LLM, apiKey, model string) flows.LLMService {
	return &service{
		client: openai.NewClient(option.WithAPIKey(apiKey)),
		llm:    llm,
		model:  model,
	}
}

func (s *service) Response(ctx context.Context, env envs.Environment, instructions, input string) (string, error) {
	resp, err := s.client.Responses.New(ctx, responses.ResponseNewParams{
		Model:        shared.ResponsesModel(s.model),
		Instructions: openai.String(instructions),
		Input: responses.ResponseNewParamsInputUnion{
			OfString: openai.String(input),
		},
		Temperature: openai.Float(0.0),
	})
	if err != nil {
		return "", fmt.Errorf("error calling OpenAI API: %w", err)
	}

	return resp.OutputText(), nil
}

var _ flows.LLMService = (*service)(nil)
