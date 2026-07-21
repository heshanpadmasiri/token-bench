package task

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
)

//go:embed prompt.md.tmpl
var sharedPromptTemplate string

// Endpoint identifies an HTTP endpoint the generated server must expose.
type Endpoint struct {
	Method string
	Path   string
}

// PromptConfig contains the common language and server details for a task prompt.
type PromptConfig struct {
	Language          string
	InitializeCommand string
	Port              int
	Endpoints         []Endpoint
	Requirements      string
}

// RenderPrompt combines the common server preamble with task-specific requirements.
func RenderPrompt(config PromptConfig) (string, error) {
	parsed, err := template.New("task-prompt").Parse(sharedPromptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse shared prompt template: %w", err)
	}
	var rendered bytes.Buffer
	if err := parsed.Execute(&rendered, config); err != nil {
		return "", fmt.Errorf("render shared prompt template: %w", err)
	}
	return rendered.String(), nil
}
