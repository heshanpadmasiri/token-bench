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
