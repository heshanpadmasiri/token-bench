package golang

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetupProject(t *testing.T) {
	directory := t.TempDir()
	implementation := New()
	if err := implementation.SetupProject(directory, 19080); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go.mod", "main.go"} {
		if _, err := os.Stat(filepath.Join(directory, name)); err != nil {
			t.Errorf("%s was not generated: %v", name, err)
		}
	}
	module, err := os.ReadFile(filepath.Join(directory, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(module), "go 1.26.5") {
		t.Fatalf("unexpected go.mod: %s", module)
	}
	main, err := os.ReadFile(filepath.Join(directory, "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(main), "const appPort = 19080") {
		t.Fatalf("unexpected main.go: %s", main)
	}
}

func TestSetupProjectRejectsInvalidPort(t *testing.T) {
	if err := New().SetupProject(t.TempDir(), 0); err == nil {
		t.Fatal("expected invalid port error")
	}
}
