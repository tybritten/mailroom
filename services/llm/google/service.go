package google

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/ai"
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
	client *genai.Client
	model  string
}

func New(m *models.LLM, c *http.Client) (flows.LLMService, error) {
	apiKey := m.Config().GetString(configAPIKey, "")
	if apiKey == "" {
		return nil, fmt.Errorf("config incomplete for LLM: %s", m.UUID())
	}

	client, err := genai.NewClient(context.Background(), &genai.ClientConfig{
		APIKey:     apiKey,
		Backend:    genai.BackendGeminiAPI,
		HTTPClient: c,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating LLM client: %w", err)
	}

	return &service{client: client, model: m.Model()}, nil
}

func (s *service) Response(ctx context.Context, instructions, input string, maxTokens int) (*flows.LLMResponse, error) {
	config := &genai.GenerateContentConfig{
		Temperature:       genai.Ptr(float32(0.000001)),
		MaxOutputTokens:   int32(maxTokens),
		SystemInstruction: &genai.Content{Parts: []*genai.Part{{Text: instructions}}}}

	resp, err := s.client.Models.GenerateContent(ctx, s.model, genai.Text(input), config)
	if err != nil {
		var apierr *genai.APIError
		if errors.As(err, &apierr) {
			if 400 <= apierr.Code && apierr.Code < 500 {
				return nil, ai.NewReasoningError(apierr.Message, instructions, input, "")
			}
			return nil, fmt.Errorf("error calling Google API: %w", err)
		}
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
