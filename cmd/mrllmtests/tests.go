package main

import (
	"context"
	"fmt"
	"slices"
	"text/template"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/ai/prompts"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

type promptTest struct {
	template       *template.Template
	data           map[string]any
	input          string
	expectedOutput []string
}

var tests = []promptTest{
	{
		template:       prompts.Translate,
		data:           map[string]any{"FromLanguage": "eng", "ToLanguage": "spa"},
		input:          "Hello @contact.name, how are you?",
		expectedOutput: []string{"Hola @contact.name, ¿cómo estás?"},
	},
	{
		template:       prompts.Translate,
		data:           map[string]any{"FromLanguage": "eng", "ToLanguage": "spa"},
		input:          "Do you still have @( fields.goats + fields.cows ) animals?",
		expectedOutput: []string{"¿Todavía tienes @( fields.goats + fields.cows ) animales?"},
	},
	{
		template:       prompts.Translate,
		data:           map[string]any{"FromLanguage": "eng", "ToLanguage": "kin"},
		input:          "Hello, how are you?",
		expectedOutput: []string{"Muraho, amakuru yawe?", "Muraho, amakuru yanyu?", "Muraho, amakuru?"},
	},
	{
		template:       prompts.Translate,
		data:           map[string]any{"FromLanguage": "eng", "ToLanguage": "xxx"},
		input:          "Hello, how are you?",
		expectedOutput: []string{"<CANT>"},
	},
	{
		template:       prompts.Categorize,
		data:           map[string]any{"arg0": "Positive, Negative, Neutral"},
		input:          "Thanks you've been very helpful",
		expectedOutput: []string{"Positive"},
	},
	{
		template:       prompts.Categorize,
		data:           map[string]any{"arg0": "Positive, Negative, Neutral"},
		input:          "Please stop sending me these messages!",
		expectedOutput: []string{"Negative"},
	},
	{
		template:       prompts.Categorize,
		data:           map[string]any{"arg0": "Positive, Negative, Neutral"},
		input:          "It was satisfactory I guess",
		expectedOutput: []string{"Neutral"},
	},
	{
		template:       prompts.Categorize,
		data:           map[string]any{"arg0": "Positive, Negative, Neutral"},
		input:          "It was satisfactory I guess",
		expectedOutput: []string{"Neutral"},
	},
}

func runPromptTests(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
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
		instructions := prompts.Render(test.template, test.data)

		fmt.Printf("======== test %d/%d =============================================\n", i+1, len(tests))
		fmt.Printf("%s\n", instructions)
		fmt.Printf("-------- input --------------------------------------------------\n")
		fmt.Printf("%s\n", test.input)
		fmt.Printf("-------- output -------------------------------------------------\n")

		for llmName, svc := range svcs {
			fmt.Printf("%s: ", llmName)
			start := time.Now()
			resp, err := svc.Response(ctx, instructions, test.input, 2500)
			if err != nil {
				fmt.Print(color(err.Error(), false))
			} else {
				allowed := slices.Contains(test.expectedOutput, resp.Output)
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
