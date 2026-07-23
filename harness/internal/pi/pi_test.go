package pi

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"token-bench/tracing"
)

func TestStartPanicsWhenTracingIsProvided(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered != "tracing is not supported by the Pi harness" {
			t.Fatalf("unexpected panic: %v", recovered)
		}
	}()
	_ = New(Config{}).Start(t.TempDir(), unsupportedBenchmark{})
}

func TestReportsConfiguredModelAndThinkingBudget(t *testing.T) {
	agent := New(Config{})
	if agent.Model() != "gpt-5.6-sol" || agent.ThinkingBudget() != "high" {
		t.Fatalf("unexpected metadata: model=%q thinkingBudget=%q", agent.Model(), agent.ThinkingBudget())
	}
}
func TestStartArgumentsUseRPCModeAndOnlyConfiguredSkills(t *testing.T) {
	agent := New(Config{SkillsDir: "/tmp/skills with 'quote"})
	expected := []string{
		"--mode", "rpc",
		"--no-session",
		"--no-skills",
		"--provider", "openai-codex",
		"--model", "gpt-5.6-sol",
		"--thinking", "high",
		"--skill", "/tmp/skills with 'quote",
	}
	if actual := agent.startArguments(); !reflect.DeepEqual(actual, expected) {
		t.Fatalf("unexpected Pi arguments:\nwant: %#v\n got: %#v", expected, actual)
	}
}

func TestStartArgumentsSupportNoSkillsBaseline(t *testing.T) {
	agent := New(Config{})
	arguments := agent.startArguments()
	if !containsArgument(arguments, "--no-skills") {
		t.Fatalf("arguments do not disable skill discovery: %#v", arguments)
	}
	if containsArgument(arguments, "--skill") {
		t.Fatalf("arguments unexpectedly load skills: %#v", arguments)
	}
}

func TestRPCProcessSupportsMultipleMessagesAndPersistsLogs(t *testing.T) {
	binDir := t.TempDir()
	piPath := filepath.Join(binDir, "pi")
	script := `#!/bin/sh
printf '%s\n' "$PWD" > pi-cwd.txt
printf '%s\n' "$@" > pi-arguments.txt
turn=0
while IFS= read -r record; do
  turn=$((turn + 1))
  case "$turn" in
    1)
      printf '%s\n' '{"id":"token-bench-1","type":"response","command":"get_state","success":true,"data":{"isStreaming":false}}'
      ;;
    2|3)
      reply=$((turn - 1))
      printf '%s\n' "{\"id\":\"token-bench-$turn\",\"type\":\"response\",\"command\":\"prompt\",\"success\":true}"
      printf '%s\n' "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"reply-$reply\"}],\"usage\":{\"input\":3,\"output\":5,\"cacheRead\":7,\"cacheWrite\":11}}}"
      printf '%s\n' '{"type":"agent_settled"}'
      ;;
  esac
done
printf '%s\n' 'fake Pi closed' >&2
`
	if err := os.WriteFile(piPath, []byte(script), 0o700); err != nil {
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
			t.Fatalf("artifact %q became visible before Pi exited: %v", name, err)
		}
	}

	cwd, err := os.ReadFile(filepath.Join(workingDir, "pi-cwd.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(cwd)) != workingDir {
		t.Fatalf("Pi ran in %q, want %q", strings.TrimSpace(string(cwd)), workingDir)
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
	if usage != (TokenCount{Input: 6, Output: 10, CacheRead: 14, CacheWrite: 22, Total: 52}) {
		t.Fatalf("unexpected usage: %+v", usage)
	}
	if err := agent.End(); err != nil {
		t.Fatal(err)
	}

	stream, err := os.ReadFile(filepath.Join(workingDir, streamArtifactName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(stream), `"type":"agent_settled"`) != 2 || !strings.Contains(string(stream), `"command":"get_state"`) {
		t.Fatalf("unexpected persisted stream: %s", stream)
	}
	stderr, err := os.ReadFile(filepath.Join(workingDir, stderrArtifactName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stderr), "fake Pi closed") {
		t.Fatalf("unexpected persisted stderr: %s", stderr)
	}
}

func TestStartReportsPrematureProcessExitAndPersistsLogs(t *testing.T) {
	binDir := t.TempDir()
	piPath := filepath.Join(binDir, "pi")
	script := "#!/bin/sh\nprintf '%s\\n' 'Pi startup failed' >&2\nexit 7\n"
	if err := os.WriteFile(piPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	workingDir := t.TempDir()
	err := New(Config{}).Start(workingDir, nil)
	if err == nil || !strings.Contains(err.Error(), "Pi startup failed") {
		t.Fatalf("unexpected startup error: %v", err)
	}
	for _, name := range []string{streamArtifactName, stderrArtifactName} {
		if _, statErr := os.Stat(filepath.Join(workingDir, name)); statErr != nil {
			t.Fatalf("startup failure did not persist %q: %v", name, statErr)
		}
	}
}

type unsupportedBenchmark struct{}

func (unsupportedBenchmark) StartTurn() tracing.TurnSpan { return nil }
func (unsupportedBenchmark) SetOK()                      {}
func (unsupportedBenchmark) SetError(string)             {}
func (unsupportedBenchmark) End()                        {}

func containsArgument(arguments []string, expected string) bool {
	for _, argument := range arguments {
		if argument == expected {
			return true
		}
	}
	return false
}
