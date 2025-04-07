package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	_ "github.com/nyaruka/mailroom/services/llm/anthropic"
	_ "github.com/nyaruka/mailroom/services/llm/openai"
	_ "github.com/nyaruka/mailroom/services/llm/openai_azure"
)

// mrllmtests is a command line tool to run LLM prompt tests against a local test database with real LLMs.
//
// go install github.com/nyaruka/mailroom/cmd/mrllmtests; mrllmtests
func main() {
	ctx := context.TODO()
	config := runtime.LoadConfig()

	slog.SetDefault(slog.New(slog.DiscardHandler)) // disable logging

	mr := mailroom.NewMailroom(config)
	err := mr.Start()
	if err != nil {
		slog.Error("unable to start mailroom", "error", err)
		os.Exit(1)
	}

	if err := runPromptTests(ctx, mr.Runtime(), models.OrgID(1)); err != nil {
		slog.Error("error running LLM tests", "error", err)
		os.Exit(1)
	}

	mr.Stop()
}
