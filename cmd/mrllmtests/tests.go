package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/ai/prompts"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

//go:embed data/tests.json
var testsJSON json.RawMessage

type promptTest struct {
	Template       string         `json:"template"`
	Data           map[string]any `json:"data"`
	Input          string         `json:"input"`
	ExpectedOutput []string       `json:"expected_output"`
}

func runPromptTests(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	var tests []promptTest
	jsonx.MustUnmarshal(testsJSON, &tests)

	oa, err := models.GetOrgAssets(ctx, rt, orgID)
	if err != nil {
		return fmt.Errorf("error loading org assets: %w", err)
	}

	llms, err := oa.LLMs()
	if err != nil {
		return fmt.Errorf("error loading LLM assets: %w", err)
	}

	svcs := make(map[string]flows.LLMService, len(llms))
	for _, llm := range llms {
		svc, err := llm.(*models.LLM).AsService()
		if err != nil {
			return fmt.Errorf("error creating LLM service for LLM '%s': %w", llm.Name(), err)
		}
		svcs[llm.Name()] = svc
	}

	for i, test := range tests {
		instructions := prompts.Render(test.Template, test.Data)

		fmt.Printf("======== test %d/%d =============================================\n", i+1, len(tests))
		fmt.Printf("%s\n", instructions)
		fmt.Printf("-------- input --------------------------------------------------\n")
		fmt.Printf("%s\n", test.Input)
		fmt.Printf("-------- output -------------------------------------------------\n")

		for llmName, svc := range svcs {
			fmt.Printf("%s: ", llmName)
			start := time.Now()
			resp, err := svc.Response(ctx, instructions, test.Input, 2500)
			if err != nil {
				fmt.Print(color(err.Error(), false))
			} else {
				allowed := slices.Contains(test.ExpectedOutput, resp.Output)
				fmt.Print(color(resp.Output, allowed))
				fmt.Printf(" [tokens=%d, time=%s]", resp.TokensUsed, time.Since(start))
			}

			fmt.Println()
		}
	}

	return nil
}

func color(msg string, success bool) string {
	const (
		Reset = "\033[0m"
		Red   = "\033[31m"
		Green = "\033[32m"
	)

	if success {
		return fmt.Sprintf("%s%s%s", Green, msg, Reset)
	}
	return fmt.Sprintf("%s%s%s", Red, msg, Reset)
}
