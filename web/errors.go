package web

import (
	"errors"

	"github.com/nyaruka/goflow/utils"
)

// ErrorResponse is the type for our error responses
type ErrorResponse struct {
	Error string            `json:"error"`
	Code  string            `json:"code,omitempty"`
	Extra map[string]string `json:"extra,omitempty"`
}

// NewErrorResponse creates a new error response from the passed in error
func NewErrorResponse(err error) *ErrorResponse {
	var rich utils.RichError
	if errors.As(err, &rich) {
		return &ErrorResponse{
			Error: rich.Error(),
			Code:  rich.Code(),
			Extra: rich.Extra(),
		}
	}
	return &ErrorResponse{Error: err.Error()}
}
