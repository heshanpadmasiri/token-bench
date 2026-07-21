package ballerina

import "testing"

func TestPromptMetadata(t *testing.T) {
	implementation := New()
	if implementation.Name() != "Ballerina" || implementation.InitializeCommand() != "bal new ." {
		t.Fatalf("unexpected prompt metadata: %q %q", implementation.Name(), implementation.InitializeCommand())
	}
}

func TestStartProjectRejectsInvalidPort(t *testing.T) {
	if _, err := New().StartProject(t.TempDir(), 0); err == nil {
		t.Fatal("expected invalid port error")
	}
}
