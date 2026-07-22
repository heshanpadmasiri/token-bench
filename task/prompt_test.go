package task

import (
	"strings"
	"testing"
)

func TestRenderPromptIncludesSharedServerPreamble(t *testing.T) {
	prompt, err := RenderPrompt(PromptConfig{
		Language:          "Go",
		InitializeCommand: "go mod init token-bench-target",
		Port:              19080,
		Endpoints: []Endpoint{
			{Method: "GET", Path: "/health"},
			{Method: "POST", Path: "/messages"},
		},
		Requirements: "task requirements",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"Go", "go mod init token-bench-target", "19080", "GET /health", "POST /messages", "task requirements"} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestBenchmarkPromptTemplateIncludesSharedAndTaskRequirements(t *testing.T) {
	prompt, err := BenchmarkPromptTemplate(NewContentBasedRouter())
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"{{.Language}}", "{{.InitializeCommand}}", "{{index .Ports 0}}", "Implement the Content-Based Router", "{{.OrdersURL}}"} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt template does not contain %q", expected)
		}
	}
}

func TestBenchmarkPromptTemplateForRejectsUnknownTask(t *testing.T) {
	if _, err := BenchmarkPromptTemplateFor("unknown"); err == nil {
		t.Fatal("expected unknown task error")
	}
}

func TestRenderPromptAssociatesEndpointsWithPorts(t *testing.T) {
	prompt, err := RenderPrompt(PromptConfig{
		Language:          "Go",
		InitializeCommand: "go mod init token-bench-target",
		Ports:             []int{19080, 19081},
		Endpoints: []Endpoint{
			{Method: "POST", Path: "/messages", PortIndex: 0},
			{Method: "POST", Path: "/messages", PortIndex: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"`POST /messages` on port `19080`", "`POST /messages` on port `19081`"} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}
