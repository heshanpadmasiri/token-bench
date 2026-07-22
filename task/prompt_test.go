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
	for _, expected := range []string{"{{.Language}}", "{{.InitializeCommand}}", "{{index .Ports 0}}", "Implement the Content-Based Router", "{{.OrdersURL}}", "{{.TaskPromptSuffix}}"} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt template does not contain %q", expected)
		}
	}
}

func TestRenderPromptAppendsTaskPromptSuffix(t *testing.T) {
	prompt, err := RenderPrompt(PromptConfig{
		Language:          "Python",
		InitializeCommand: "uv init .",
		Port:              19080,
		Requirements:      "task requirements",
		TaskPromptSuffix:  "## Python project requirements\n\nUse uv.",
	})
	if err != nil {
		t.Fatal(err)
	}
	requirementsIndex := strings.Index(prompt, "task requirements")
	suffixIndex := strings.Index(prompt, "## Python project requirements")
	if requirementsIndex < 0 || suffixIndex <= requirementsIndex {
		t.Fatalf("task prompt suffix was not appended after requirements:\n%s", prompt)
	}
}

func TestRenderPromptOmitsEmptyTaskPromptSuffixWithoutExtraWhitespace(t *testing.T) {
	prompt, err := RenderPrompt(PromptConfig{
		Language:          "Go",
		InitializeCommand: "go mod init token-bench-target",
		Port:              19080,
		Requirements:      "task requirements",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(prompt, "task requirements\n") {
		t.Fatalf("empty suffix changed prompt ending: %q", prompt)
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
