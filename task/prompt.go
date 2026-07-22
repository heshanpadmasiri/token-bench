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
	Method    string
	Path      string
	PortIndex int
}

// PromptConfig contains the common language and server details for a task prompt.
type PromptConfig struct {
	Language          string
	InitializeCommand string
	Port              int // Deprecated: use Ports. Retained for callers defining a single listener.
	Ports             []int
	Endpoints         []Endpoint
	Requirements      string
}

type renderedEndpoint struct {
	Method string
	Path   string
	Port   int
}

// RenderPrompt combines the common server preamble with task-specific requirements.
func RenderPrompt(config PromptConfig) (string, error) {
	ports := config.Ports
	if len(ports) == 0 {
		ports = []int{config.Port}
	}
	endpoints := make([]renderedEndpoint, 0, len(config.Endpoints))
	for _, endpoint := range config.Endpoints {
		if endpoint.PortIndex < 0 || endpoint.PortIndex >= len(ports) {
			return "", fmt.Errorf("endpoint %s %s references unavailable port index %d", endpoint.Method, endpoint.Path, endpoint.PortIndex)
		}
		endpoints = append(endpoints, renderedEndpoint{Method: endpoint.Method, Path: endpoint.Path, Port: ports[endpoint.PortIndex]})
	}
	data := struct {
		Language          string
		InitializeCommand string
		Endpoints         []renderedEndpoint
		Requirements      string
	}{config.Language, config.InitializeCommand, endpoints, config.Requirements}
	parsed, err := template.New("task-prompt").Parse(sharedPromptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse shared prompt template: %w", err)
	}
	var rendered bytes.Buffer
	if err := parsed.Execute(&rendered, data); err != nil {
		return "", fmt.Errorf("render shared prompt template: %w", err)
	}
	return rendered.String(), nil
}
