package ballerina

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
	for _, name := range []string{"Ballerina.toml", "Config.toml", "main.bal"} {
		if _, err := os.Stat(filepath.Join(directory, name)); err != nil {
			t.Errorf("%s was not generated: %v", name, err)
		}
	}
	config, err := os.ReadFile(filepath.Join(directory, "Config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), "appPort = 19080") {
		t.Fatalf("unexpected config: %s", config)
	}
}
