package openai

import (
	"context"
	"fmt"
	"strings"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"google.golang.org/genai"
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
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  s.apiKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Google genai client: %w", err)
	}

	config := &genai.GenerateContentConfig{
		Temperature:       genai.Ptr(float32(0.000001)),
		MaxOutputTokens:   int32(maxTokens),
		SystemInstruction: &genai.Content{Parts: []*genai.Part{{Text: instructions}}}}

	resp, err := client.Models.GenerateContent(ctx, s.model, genai.Text(input), config)
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
