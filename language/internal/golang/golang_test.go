package golang

import "testing"

func TestPromptMetadata(t *testing.T) {
	implementation := New()
	if implementation.Name() != "Go" || implementation.InitializeCommand() != "go mod init token-bench-target" {
		t.Fatalf("unexpected prompt metadata: %q %q", implementation.Name(), implementation.InitializeCommand())
	}
	if implementation.TaskPromptSuffix() != "" {
		t.Fatalf("unexpected task prompt suffix: %q", implementation.TaskPromptSuffix())
	}
}

func TestStartProjectRejectsInvalidPort(t *testing.T) {
	if _, err := New().StartProject(t.TempDir(), 0); err == nil {
		t.Fatal("expected invalid port error")
	}
}
