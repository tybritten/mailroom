package prompts

import (
	_ "embed"
	"strings"
	"text/template"

	"github.com/nyaruka/mailroom/core/goflow"
)

//go:embed templates/categorize.txt
var categorize string
var Categorize = template.Must(template.New("").Parse(categorize))

//go:embed templates/translate.txt
var translate string
var Translate = template.Must(template.New("").Parse(translate))

//go:embed templates/translate_unknown_from.txt
var translateUnknownFrom string
var TranslateUnknownFrom = template.Must(template.New("").Parse(translateUnknownFrom))

func init() {
	goflow.RegisterLLMPrompts(map[string]*template.Template{
		"categorize":             Categorize,
		"translate":              Translate,
		"translate_unknown_from": TranslateUnknownFrom,
	})
}

// Render is a helper function to render a template with the given data.
func Render(tpl *template.Template, data any) string {
	var out strings.Builder
	if err := tpl.Execute(&out, data); err != nil {
		panic(err)
	}
	return out.String()
}
