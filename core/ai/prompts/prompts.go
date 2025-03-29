package prompts

import (
	_ "embed"
	"strings"
	"text/template"
)

//go:embed templates/translate.txt
var translate string
var Translate = template.Must(template.New("translate").Parse(translate))

//go:embed templates/translate_unknown_from.txt
var translateUnknownFrom string
var TranslateUnknownFrom = template.Must(template.New("translate").Parse(translateUnknownFrom))

func Render(tpl *template.Template, data any) string {
	var out strings.Builder
	if err := tpl.Execute(&out, data); err != nil {
		panic(err)
	}
	return out.String()
}
