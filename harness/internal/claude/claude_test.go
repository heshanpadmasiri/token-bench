package claude

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStartArgumentsUseStreamingPrintMode(t *testing.T) {
	agent := New(Config{})
	agent.pluginPath = "/tmp/plugin with spaces"

	expected := []string{
		"--model", "opus",
		"--effort", "high",
		"--dangerously-skip-permissions",
		"--setting-sources", "",
		"-p",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		"--plugin-dir", "/tmp/plugin with spaces",
	}
	if actual := agent.startArguments(); !reflect.DeepEqual(actual, expected) {
		t.Fatalf("unexpected Claude arguments:\nwant: %#v\n got: %#v", expected, actual)
	}
}

func TestStreamingProcessSupportsMultipleMessagesAndPersistsLogs(t *testing.T) {
	binDir := t.TempDir()
	claudePath := filepath.Join(binDir, "claude")
	script := `#!/bin/sh
turn=0
while IFS= read -r message; do
  turn=$((turn + 1))
  if [ "$turn" -eq 1 ]; then
    printf '%s\n' '{"type":"system","subtype":"init"}'
  fi
  printf '%s\n' "{\"type\":\"result\",\"subtype\":\"success\",\"result\":\"reply-$turn\",\"modelUsage\":{\"main\":{\"inputTokens\":3,\"outputTokens\":5,\"cacheReadInputTokens\":7,\"cacheCreationInputTokens\":11},\"subagent\":{\"inputTokens\":13,\"outputTokens\":17,\"cacheReadInputTokens\":19,\"cacheCreationInputTokens\":23}}}"
done
printf '%s\n' 'fake Claude closed' >&2
`
	if err := os.WriteFile(claudePath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	workingDir := t.TempDir()
	agent := New(Config{})
	if err := agent.Start(workingDir, nil); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{streamArtifactName, stderrArtifactName} {
		if _, err := os.Stat(filepath.Join(workingDir, name)); !os.IsNotExist(err) {
			t.Fatalf("artifact %q became visible before Claude exited: %v", name, err)
		}
	}

	for index, message := range []string{"initial prompt", "correction"} {
		reply, err := agent.SendMessage(message)
		if err != nil {
			t.Fatal(err)
		}
		expected := "reply-" + string(rune('1'+index))
		if reply != expected {
			t.Fatalf("unexpected reply %q, want %q", reply, expected)
		}
	}
	usage, err := agent.TokenCount()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Input != 32 || usage.Output != 44 || usage.CacheRead != 52 || usage.CacheWrite != 68 || usage.Total != 196 {
		t.Fatalf("unexpected subagent-inclusive usage: %+v", usage)
	}
	if err := agent.End(); err != nil {
		t.Fatal(err)
	}

	stream, err := os.ReadFile(filepath.Join(workingDir, streamArtifactName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(stream), `"type":"result"`) != 2 || !strings.Contains(string(stream), `"subagent"`) {
		t.Fatalf("unexpected persisted stream: %s", stream)
	}
	stderr, err := os.ReadFile(filepath.Join(workingDir, stderrArtifactName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stderr), "fake Claude closed") {
		t.Fatalf("unexpected persisted stderr: %s", stderr)
	}
}

func TestResultUsageFallsBackWhenModelUsageIsUnavailable(t *testing.T) {
	agent := New(Config{})
	agent.addUsage(resultEvent{Usage: resultUsage{Input: 2, Output: 3, CacheRead: 5, CacheWrite: 7}})
	if agent.tokens != (TokenCount{Input: 2, Output: 3, CacheRead: 5, CacheWrite: 7, Total: 17}) {
		t.Fatalf("unexpected fallback usage: %+v", agent.tokens)
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
	defer agent.cleanupPlugin()
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
	defer agent.cleanupPlugin()
	entries, err := os.ReadDir(filepath.Join(agent.pluginPath, "skills"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one staged skill, got %d", len(entries))
	}
}
