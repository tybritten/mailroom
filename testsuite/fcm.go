package testsuite

import (
	"context"
	"errors"

	"firebase.google.com/go/v4/messaging"
	"github.com/nyaruka/goflow/utils"
)

type MockFCMService struct {
	tokens []string

	// log of messages sent to this endpoint
	Messages []*messaging.Message
}

func (m *MockFCMService) Client(ctx context.Context, androidFCMServiceAccountFile string) *MockFCMClient {
	return &MockFCMClient{FCMService: m}
}

func (m *MockFCMService) GetClient(ctx context.Context) *MockFCMClient {
	return m.Client(ctx, "testfiles/android.json")
}

func NewMockFCMService(tokens ...string) *MockFCMService {
	mock := &MockFCMService{tokens: tokens}
	return mock
}

type MockFCMClient struct {
	FCMService *MockFCMService
}

func (fc *MockFCMClient) Send(ctx context.Context, messages ...*messaging.Message) (*messaging.BatchResponse, error) {
	successCount := 0
	failureCount := 0
	sendResponses := make([]*messaging.SendResponse, len(messages))
	var err error

	for _, message := range messages {
		fc.FCMService.Messages = append(fc.FCMService.Messages, message)

		if utils.StringSliceContains(fc.FCMService.tokens, message.Token, false) {
			successCount += 1
			sendResponses = append(sendResponses, &messaging.SendResponse{Success: true})
		} else {
			failureCount += 1
			err = errors.New("401 error: 401 Unauthorized")
			sendResponses = append(sendResponses, &messaging.SendResponse{Error: err})
		}
	}
	return &messaging.BatchResponse{SuccessCount: successCount, FailureCount: failureCount, Responses: sendResponses}, err
}
