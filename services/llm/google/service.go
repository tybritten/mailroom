package openai

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/generative-ai-go/genai"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"google.golang.org/api/option"
)

const (
	TypeGoogle = "google"

	configAPIKey = "api_key"
)

func init() {
	models.RegisterLLMService(TypeGoogle, New)
}

// an LLM service implementation for Google GenAI
type service struct {
	apiKey string
	model  string
}

func New(m *models.LLM) (flows.LLMService, error) {
	apiKey := m.Config().GetString(configAPIKey, "")
	if apiKey == "" {
		return nil, fmt.Errorf("config incomplete for LLM: %s", m.UUID())
	}

	return &service{apiKey: apiKey, model: m.Model()}, nil
}

func (s *service) Response(ctx context.Context, instructions, input string, maxTokens int) (*flows.LLMResponse, error) {
	client, err := genai.NewClient(ctx, option.WithAPIKey(s.apiKey))
	if err != nil {
		return nil, fmt.Errorf("error creating Google genai client: %w", err)
	}
	defer client.Close()

	model := client.GenerativeModel(s.model)
	model.SetTemperature(0.000001)
	model.SetMaxOutputTokens(int32(maxTokens))
	model.SystemInstruction = &genai.Content{
		Parts: []genai.Part{genai.Text(instructions)},
	}

	resp, err := model.GenerateContent(ctx, genai.Text(input))
	if err != nil {
		return nil, fmt.Errorf("error calling Google API: %w", err)
	}

	var output strings.Builder
	for _, candidate := range resp.Candidates {
		if candidate.Content != nil {
			output.WriteString(fmt.Sprint(candidate.Content.Parts[0]))
		}
	}

	return &flows.LLMResponse{
		Output:     strings.TrimSpace(output.String()),
		TokensUsed: int64(resp.UsageMetadata.TotalTokenCount),
	}, nil
}
