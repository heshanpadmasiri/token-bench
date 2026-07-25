package task

import (
	"strings"
	"testing"
)

func TestTaskDefinitions(t *testing.T) {
	tests := []struct {
		definition Task
		name       string
		ports      int
		endpoints  []Endpoint
	}{
		{NewClaimCheck(), "claim-check", 5, []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/messages"}, {Method: "GET", Path: "/health", PortIndex: 1}, {Method: "POST", Path: "/messages", PortIndex: 1}}},
		{NewContentBasedRouter(), "content-based-router", 3, []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/messages"}}},
		{NewContentEnricher(), "content-enricher", 3, []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/messages"}}},
		{NewDeadLetterChannel(), "dead-letter-channel", 2, []Endpoint{{Method: "GET", Path: "/health"}}},
		{NewMultiConsumer(), "multi-consumer", 3, []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/orders"}}},
		{NewScatterGather(), "scatter-gather", 4, []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/quotes"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := test.definition
			if definition.Name != test.name || definition.Resources.NPorts != test.ports || definition.InitFn == nil || definition.RequirementsTemplate == "" {
				t.Fatalf("invalid task definition: %+v", definition)
			}
			if len(definition.Endpoints) != len(test.endpoints) {
				t.Fatalf("endpoints = %+v, want %+v", definition.Endpoints, test.endpoints)
			}
			for index, endpoint := range definition.Endpoints {
				if endpoint != test.endpoints[index] {
					t.Errorf("endpoint %d = %+v, want %+v", index, endpoint, test.endpoints[index])
				}
			}
		})
	}
}

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
