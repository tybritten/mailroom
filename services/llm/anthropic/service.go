package anthropic

import (
	"context"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
)

// an LLM service implementation for Anthropic
type service struct {
	client anthropic.Client
	llm    *flows.LLM
	model  string
}

// NewService creates a new classification service
func NewService(llm *flows.LLM, apiKey, model string) flows.LLMService {
	return &service{
		client: anthropic.NewClient(option.WithAPIKey(apiKey)),
		llm:    llm,
		model:  model,
	}
}

func (s *service) Response(ctx context.Context, env envs.Environment, instructions, input string) (*flows.LLMResponse, error) {
	resp, err := s.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model: anthropic.Model(s.model),
		Messages: []anthropic.MessageParam{
			{
				Role: anthropic.MessageParamRoleAssistant,
				Content: []anthropic.ContentBlockParamUnion{
					{
						OfRequestTextBlock: &anthropic.TextBlockParam{Text: instructions},
					},
				},
			},
			{
				Role: anthropic.MessageParamRoleUser,
				Content: []anthropic.ContentBlockParamUnion{
					{
						OfRequestTextBlock: &anthropic.TextBlockParam{Text: input},
					},
				},
			},
		},
		Temperature: anthropic.Float(0.0),
	})
	if err != nil {
		return nil, fmt.Errorf("error calling Anthropic API: %w", err)
	}

	var output strings.Builder
	for _, content := range resp.Content {
		if content.Type == "text" {
			output.WriteString(content.Text)
		}
	}

	return &flows.LLMResponse{
		Output:     output.String(),
		TokensUsed: resp.Usage.InputTokens + resp.Usage.OutputTokens,
	}, nil
}

var _ flows.LLMService = (*service)(nil)
