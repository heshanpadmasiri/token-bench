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
	TaskPromptSuffix  string
	Port              int // Deprecated: use Ports. Retained for callers defining a single listener.
	Ports             []int
	Endpoints         []Endpoint
	Requirements      string
}

type renderedEndpoint struct {
	Method string
	Path   string
	Port   string
}

type promptData struct {
	Language          string
	InitializeCommand string
	TaskPromptSuffix  string
	Endpoints         []renderedEndpoint
	Requirements      string
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
		endpoints = append(endpoints, renderedEndpoint{Method: endpoint.Method, Path: endpoint.Path, Port: fmt.Sprint(ports[endpoint.PortIndex])})
	}
	return renderPrompt(promptData{
		Language:          config.Language,
		InitializeCommand: config.InitializeCommand,
		TaskPromptSuffix:  config.TaskPromptSuffix,
		Endpoints:         endpoints,
		Requirements:      config.Requirements,
	})
}

// BenchmarkPromptTemplate returns the complete unrendered prompt for a task.
func BenchmarkPromptTemplate(definition Task) (string, error) {
	endpoints := make([]renderedEndpoint, 0, len(definition.Endpoints))
	for _, endpoint := range definition.Endpoints {
		if endpoint.PortIndex < 0 || endpoint.PortIndex >= definition.Resources.NPorts {
			return "", fmt.Errorf("endpoint %s %s references unavailable port index %d", endpoint.Method, endpoint.Path, endpoint.PortIndex)
		}
		endpoints = append(endpoints, renderedEndpoint{
			Method: endpoint.Method,
			Path:   endpoint.Path,
			Port:   fmt.Sprintf("{{index .Ports %d}}", endpoint.PortIndex),
		})
	}
	return renderPrompt(promptData{
		Language:          "{{.Language}}",
		InitializeCommand: "{{.InitializeCommand}}",
		TaskPromptSuffix:  "{{.TaskPromptSuffix}}",
		Endpoints:         endpoints,
		Requirements:      definition.RequirementsTemplate,
	})
}

// BenchmarkPromptTemplateFor returns the current embedded prompt template for a named task.
func BenchmarkPromptTemplateFor(name string) (string, error) {
	var definition Task
	switch name {
	case "claim-check":
		definition = NewClaimCheck()
	case "content-based-router":
		definition = NewContentBasedRouter()
	case "content-enricher":
		definition = NewContentEnricher()
	case "dead-letter-channel":
		definition = NewDeadLetterChannel()
	case "scatter-gather":
		definition = NewScatterGather()
	default:
		return "", fmt.Errorf("unknown task %q", name)
	}
	return BenchmarkPromptTemplate(definition)
}

func renderPrompt(data promptData) (string, error) {
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
