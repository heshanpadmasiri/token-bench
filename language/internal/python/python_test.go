package python

import (
	"strings"
	"testing"
)

func TestPromptMetadata(t *testing.T) {
	implementation := New()
	if implementation.Name() != "Python" {
		t.Fatalf("unexpected name: %q", implementation.Name())
	}
	const initializeCommand = "uv init --app --name token-bench-target --no-readme --vcs none --python 3.12 ."
	if implementation.InitializeCommand() != initializeCommand {
		t.Fatalf("unexpected initialization command: %q", implementation.InitializeCommand())
	}
	for _, expected := range []string{"uv", "pyproject.toml", "main.py", "uv run python main.py"} {
		if !strings.Contains(implementation.TaskPromptSuffix(), expected) {
			t.Errorf("task prompt suffix does not contain %q", expected)
		}
	}
}

func TestStartProjectRejectsInvalidPort(t *testing.T) {
	if _, err := New().StartProject(t.TempDir(), 0); err == nil {
		t.Fatal("expected invalid port error")
	}
}
