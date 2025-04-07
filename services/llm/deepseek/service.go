package openai

import (
	"context"
	"fmt"
	"strings"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/shared"
)

const (
	TypeDeepSeek = "deepseek"

	configAPIKey = "api_key"
	configModel  = "model"
)

func init() {
	models.RegisterLLMService(TypeDeepSeek, New)
}

// an LLM service implementation for DeepSeek
type service struct {
	client openai.Client
	model  string
}

func New(m *models.LLM) (flows.LLMService, error) {
	apiKey := m.Config().GetString(configAPIKey, "")
	model := m.Config().GetString(configModel, "")
	if apiKey == "" || model == "" {
		return nil, fmt.Errorf("config incomplete for LLM: %s", m.UUID())
	}

	return &service{
		client: openai.NewClient(option.WithBaseURL("https://api.deepseek.com"), option.WithAPIKey(apiKey)),
		model:  model,
	}, nil
}

func (s *service) Response(ctx context.Context, instructions, input string, maxTokens int) (*flows.LLMResponse, error) {
	resp, err := s.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: shared.ChatModel(s.model),
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(instructions),
			openai.UserMessage(input),
		},
		Temperature: openai.Float(0.000001),
		MaxTokens:   openai.Int(int64(maxTokens)),
	})
	if err != nil {
		return nil, fmt.Errorf("error calling DeepSeek API: %w", err)
	}

	return &flows.LLMResponse{
		Output:     strings.TrimSpace(resp.Choices[0].Message.Content),
		TokensUsed: resp.Usage.TotalTokens,
	}, nil
}
