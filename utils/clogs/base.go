package clogs

import (
	"fmt"

	"github.com/nyaruka/gocommon/uuids"
)

// LogUUID is the type of a channel log UUID (should be v7)
type LogUUID uuids.UUID

// NewLogUUID creates a new channel log UUID
func NewLogUUID() LogUUID {
	return LogUUID(uuids.NewV7())
}

// Error is an error that occurred during a channel interaction
type LogError struct {
	Code    string `json:"code"`
	ExtCode string `json:"ext_code,omitempty"`
	Message string `json:"message"`
}

func NewLogError(code, extCode, message string, args ...any) *LogError {
	return &LogError{Code: code, ExtCode: extCode, Message: fmt.Sprintf(message, args...)}
}
