package pi

import (
	"strings"
	"testing"
)

func TestStartCommandUsesOnlyConfiguredSkills(t *testing.T) {
	agent := New(Config{SkillsDir: "/tmp/skills with 'quote"})
	agent.stream = "/tmp/rpc stream"
	command := agent.startCommand()

	for _, expected := range []string{"--no-skills", "--skill '/tmp/skills with '\\''quote'", "tee -a '/tmp/rpc stream'"} {
		if !strings.Contains(command, expected) {
			t.Errorf("command does not contain %q: %s", expected, command)
		}
	}
}

func TestStartCommandSupportsNoSkillsBaseline(t *testing.T) {
	agent := New(Config{})
	command := agent.startCommand()
	if !strings.Contains(command, "--no-skills") {
		t.Fatalf("command does not disable skill discovery: %s", command)
	}
	if strings.Contains(command, "--skill ") {
		t.Fatalf("command unexpectedly loads skills: %s", command)
	}
}
