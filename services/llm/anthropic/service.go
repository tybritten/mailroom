package anthropic

import (
	"context"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
)

const (
	TypeAnthropic = "anthropic"

	configAPIKey = "api_key"
	configModel  = "model"
)

func init() {
	models.RegisterLLMService(TypeAnthropic, New)
}

// an LLM service implementation for Anthropic
type service struct {
	client anthropic.Client
	model  string
}

func New(m *models.LLM) (flows.LLMService, error) {
	apiKey := m.Config().GetString(configAPIKey, "")
	model := m.Config().GetString(configModel, "")
	if apiKey == "" || model == "" {
		return nil, fmt.Errorf("config incomplete for LLM: %s", m.UUID())
	}

	return &service{
		client: anthropic.NewClient(option.WithAPIKey(apiKey)),
		model:  model,
	}, nil
}

func (s *service) Response(ctx context.Context, instructions, input string, maxTokens int) (*flows.LLMResponse, error) {
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
		MaxTokens:   2500,
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

	return &flows.LLMResponse{Output: output.String(), TokensUsed: resp.Usage.InputTokens + resp.Usage.OutputTokens}, nil
}
