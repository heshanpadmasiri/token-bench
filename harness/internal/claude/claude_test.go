package claude

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartCommandUsesInteractiveClaude(t *testing.T) {
	agent := New(Config{})
	agent.settingsPath = "/tmp/settings with 'quote.json"
	agent.pluginPath = "/tmp/plugin with spaces"
	command := agent.startCommand()

	for _, expected := range []string{
		"claude --model 'opus'",
		"--effort 'high'",
		"--dangerously-skip-permissions",
		"--setting-sources ''",
		"--settings '/tmp/settings with '\\''quote.json'",
		"--plugin-dir '/tmp/plugin with spaces'",
	} {
		if !strings.Contains(command, expected) {
			t.Errorf("command does not contain %q: %s", expected, command)
		}
	}
	if strings.Contains(command, " -p") || strings.Contains(command, "--print") {
		t.Fatalf("command unexpectedly uses print mode: %s", command)
	}
}

func TestParseTranscriptDeduplicatesStreamingRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	transcript := strings.Join([]string{
		`{"type":"user","message":{"role":"user","content":"hello"}}`,
		`{"type":"assistant","uuid":"first-a","message":{"id":"msg-1","role":"assistant","content":[{"type":"text","text":"Working"}],"usage":{"input_tokens":3,"output_tokens":2,"cache_read_input_tokens":5,"cache_creation_input_tokens":7}}}`,
		`{"type":"assistant","uuid":"first-b","message":{"id":"msg-1","role":"assistant","content":[{"type":"text","text":"Working"},{"type":"tool_use","id":"tool-1"}],"usage":{"input_tokens":3,"output_tokens":2,"cache_read_input_tokens":5,"cache_creation_input_tokens":7}}}`,
		`{"type":"assistant","uuid":"second","message":{"id":"msg-2","role":"assistant","content":[{"type":"text","text":"Done"}],"usage":{"input_tokens":11,"output_tokens":13,"cache_read_input_tokens":17,"cache_creation_input_tokens":19}}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
		t.Fatal(err)
	}

	reply, tokens, err := parseTranscript(path)
	if err != nil {
		t.Fatal(err)
	}
	if reply != "Done" {
		t.Fatalf("unexpected reply %q", reply)
	}
	if tokens.Input != 14 || tokens.Output != 15 || tokens.CacheRead != 22 || tokens.CacheWrite != 26 || tokens.Total != 77 {
		t.Fatalf("unexpected tokens: %+v", tokens)
	}
}

func TestCreateSkillsPluginSupportsSkillCollections(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"review", "testing"} {
		directory := filepath.Join(root, name)
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "SKILL.md"), []byte("---\nname: "+name+"\n---\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	agent := New(Config{SkillsDir: root})
	if err := agent.createSkillsPlugin(); err != nil {
		t.Fatal(err)
	}
	defer agent.cleanupFiles()
	for _, name := range []string{"review", "testing"} {
		path := filepath.Join(agent.pluginPath, "skills", name)
		if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("skill %q was not staged as a symlink: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(agent.pluginPath, ".claude-plugin", "plugin.json")); err != nil {
		t.Fatalf("plugin manifest was not created: %v", err)
	}
}

func TestCreateSkillsPluginSupportsSingleSkillDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "SKILL.md"), []byte("---\nname: single\n---\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	agent := New(Config{SkillsDir: root})
	if err := agent.createSkillsPlugin(); err != nil {
		t.Fatal(err)
	}
	defer agent.cleanupFiles()
	entries, err := os.ReadDir(filepath.Join(agent.pluginPath, "skills"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one staged skill, got %d", len(entries))
	}
}

func TestCreateHookFilesConfiguresStopHook(t *testing.T) {
	agent := New(Config{})
	if err := agent.createHookFiles(); err != nil {
		t.Fatal(err)
	}
	defer agent.cleanupFiles()

	settings, err := os.ReadFile(agent.settingsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(settings), `"Stop"`) || !strings.Contains(string(settings), agent.eventsPath) {
		t.Fatalf("settings do not configure Stop hook: %s", settings)
	}
}
