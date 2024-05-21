package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/nyaruka/mailroom/runtime"
)

type JSONHandler[T any] func(ctx context.Context, rt *runtime.Runtime, request *T) (any, int, error)

func JSONPayload[T any](handler JSONHandler[T]) Handler {
	return MarshaledResponse(func(ctx context.Context, rt *runtime.Runtime, r *http.Request) (any, int, error) {
		payload := new(T)

		if err := ReadAndValidateJSON(r, payload); err != nil {
			return fmt.Errorf("request failed validation: %w", err), http.StatusBadRequest, nil
		}

		return handler(ctx, rt, payload)
	})
}

type MarshaledHandler func(ctx context.Context, rt *runtime.Runtime, r *http.Request) (any, int, error)

// MarshaledResponse wraps a handler to change the signature so that the return value is marshaled as the response
func MarshaledResponse(handler MarshaledHandler) Handler {
	return func(ctx context.Context, rt *runtime.Runtime, r *http.Request, w http.ResponseWriter) error {
		value, status, err := handler(ctx, rt, r)
		if err != nil {
			return err
		}

		// handler returned an error to use as the response
		asError, isError := value.(error)
		if isError {
			value = NewErrorResponse(asError)
		}

		return WriteMarshalled(w, status, value)
	}
}

// RequireAuthToken wraps a handler to require that our request to have our global authorization header
func RequireAuthToken(handler Handler) Handler {
	return func(ctx context.Context, rt *runtime.Runtime, r *http.Request, w http.ResponseWriter) error {
		auth := r.Header.Get("authorization")

		if rt.Config.AuthToken != "" && fmt.Sprintf("Token %s", rt.Config.AuthToken) != auth {
			return WriteMarshalled(w, http.StatusUnauthorized, NewErrorResponse(errors.New("invalid or missing authorization header")))
		}

		// we are authenticated, call our chain
		return handler(ctx, rt, r, w)
	}
}
