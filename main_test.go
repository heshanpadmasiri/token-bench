package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"token-bench/harness"
	benchresult "token-bench/result"
)

func TestDefaultSIGINTSkipsDeferredCleanup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGINT process semantics differ on Windows")
	}
	if os.Getenv("TOKEN_BENCH_SIGINT_HELPER") == "1" {
		marker := os.Getenv("TOKEN_BENCH_SIGINT_MARKER")
		defer func() { _ = os.WriteFile(marker, []byte("cleanup ran"), 0o644) }()
		if err := os.WriteFile(os.Getenv("TOKEN_BENCH_SIGINT_READY"), []byte("ready"), 0o644); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}

	directory := t.TempDir()
	marker := filepath.Join(directory, "cleanup")
	ready := filepath.Join(directory, "ready")
	command := exec.Command(os.Args[0], "-test.run=^TestDefaultSIGINTSkipsDeferredCleanup$")
	command.Env = append(os.Environ(),
		"TOKEN_BENCH_SIGINT_HELPER=1",
		"TOKEN_BENCH_SIGINT_MARKER="+marker,
		"TOKEN_BENCH_SIGINT_READY="+ready,
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	finished := false
	t.Cleanup(func() {
		if !finished {
			_ = command.Process.Kill()
			<-waited
		}
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case err := <-waited:
			finished = true
			t.Fatalf("helper exited before SIGINT: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("helper did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		finished = true
		if err == nil {
			t.Fatal("helper exited successfully after SIGINT")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("helper did not terminate immediately after SIGINT")
	}
	if content, err := os.ReadFile(marker); err == nil {
		t.Fatalf("deferred cleanup ran after SIGINT: %q", content)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestParseOptions(t *testing.T) {
	parsed, err := parseOptions([]string{"content-based-router", "ballerina", "pi", "--n-runs=3", "--target=/tmp/token-bench", "--skills=/tmp/skills"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.runs != 3 || parsed.target != "/tmp/token-bench" || parsed.skillsDir != "/tmp/skills" {
		t.Fatalf("unexpected options: %+v", parsed)
	}
}

func TestParsePhoenixEndpoint(t *testing.T) {
	parsed, err := parseOptions([]string{"claim-check", "go", "claude", "--phoenix-endpoint", "http://localhost:6006"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.phoenixEndpoint != "http://localhost:6006" {
		t.Fatalf("unexpected Phoenix endpoint: %q", parsed.phoenixEndpoint)
	}
	for _, arguments := range [][]string{
		{"claim-check", "go", "claude", "--phoenix-endpoint="},
		{"claim-check", "go", "claude", "--phoenix-endpoint", "http://localhost:6006", "--phoenix-endpoint=http://localhost:6007"},
	} {
		if _, err := parseOptions(arguments); err == nil {
			t.Errorf("parseOptions(%q) unexpectedly succeeded", arguments)
		}
	}
}

func TestNormalizeSkillsDir(t *testing.T) {
	directory := t.TempDir()
	normalized, err := normalizeSkillsDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if normalized != filepath.Clean(directory) || !filepath.IsAbs(normalized) {
		t.Fatalf("unexpected normalized directory: %q", normalized)
	}

	file := filepath.Join(directory, "SKILL.md")
	if err := writeTestFile(file); err != nil {
		t.Fatal(err)
	}
	if _, err := normalizeSkillsDir(file); err == nil {
		t.Fatal("expected non-directory skills path to fail")
	}
}

func writeTestFile(path string) error {
	return os.WriteFile(path, []byte("skill"), 0o644)
}

func TestSelectTask(t *testing.T) {
	for _, name := range []string{"claim-check", "content-based-router", "content-enricher", "dead-letter-channel", "multi-consumer", "scatter-gather"} {
		if _, err := selectTask(name); err != nil {
			t.Errorf("selectTask(%q): %v", name, err)
		}
	}
	if _, err := selectTask("unknown"); err == nil {
		t.Fatal("expected unknown task error")
	}
}

func TestSelectLanguage(t *testing.T) {
	for _, name := range []string{"ballerina", "go", "java", "python"} {
		if _, err := selectLanguage(name); err != nil {
			t.Errorf("selectLanguage(%q): %v", name, err)
		}
	}
	if _, err := selectLanguage("unknown"); err == nil {
		t.Fatal("expected unknown language error")
	}
}

func TestSelectHarness(t *testing.T) {
	for _, name := range []string{"claude", "pi"} {
		if _, err := selectHarness(name, harness.Config{}); err != nil {
			t.Errorf("selectHarness(%q): %v", name, err)
		}
	}
	if _, err := selectHarness("unknown", harness.Config{}); err == nil {
		t.Fatal("expected unknown harness error")
	}
}

func TestRunDirectoryPrefixNormalizesWhitespace(t *testing.T) {
	prefix := runDirectoryPrefix("Go lang", "content\tbased\nrouter", "pi\u00a0agent")
	if prefix != "Go_lang-content_based_router-pi_agent-" {
		t.Fatalf("unexpected run directory prefix: %q", prefix)
	}
}

func TestCreateRunDirectoryUsesUniqueNames(t *testing.T) {
	target := t.TempDir()
	first, err := createRunDirectory(target, "go", "content based router", "pi")
	if err != nil {
		t.Fatal(err)
	}
	second, err := createRunDirectory(target, "go", "content based router", "pi")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("run directories are not unique: %q", first)
	}
	for _, directory := range []string{first, second} {
		if !strings.HasPrefix(filepath.Base(directory), "go-content_based_router-pi-") {
			t.Errorf("unexpected run directory name: %q", directory)
		}
		info, err := os.Stat(directory)
		if err != nil {
			t.Error(err)
		} else if !info.IsDir() {
			t.Errorf("run path is not a directory: %q", directory)
		}
	}
}

func TestSummarize(t *testing.T) {
	runs := []runResult{
		{Tokens: harnessTokenCount(10, 20)},
		{Tokens: harnessTokenCount(30, 40)},
	}
	mean, median := summarize(runs)
	if mean.Input != 20 || median.Output != 30 {
		t.Fatalf("unexpected summary: mean=%+v median=%+v", mean, median)
	}
}

func TestRenderReportForFinalTUI(t *testing.T) {
	result := report{
		Task: "content-based-router", Language: "go", Harness: "pi",
		Runs: []runResult{{Run: 1, Passed: true, Tokens: harnessTokenCount(10, 20)}},
	}
	rendered, err := renderReport(result, "/tmp/results")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"RUN", "PASS", "1", "true", "JSON results:", "go-content-based-router-pi-*"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered report does not contain %q:\n%s", expected, rendered)
		}
	}
}

func TestHiddenValidationFailureIsReportedWithoutFailingBenchmarkProcess(t *testing.T) {
	runs := []runResult{{Run: 1, Passed: false, Error: hiddenValidationFailure}}
	if exitCode := benchmarkExitCode(runs); exitCode != 0 {
		t.Fatalf("benchmarkExitCode() = %d, want 0", exitCode)
	}

	rendered, err := renderReport(report{
		Task: "content-based-router", Language: "go", Harness: "pi", Runs: runs,
	}, "/tmp/results")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered, hiddenValidationFailure) {
		t.Fatalf("rendered report does not state hidden validation failure:\n%s", rendered)
	}
}

func TestOperationalFailureFailsBenchmarkProcess(t *testing.T) {
	runs := []runResult{{Run: 1, Passed: false, Error: "application failed to start"}}
	if exitCode := benchmarkExitCode(runs); exitCode != 1 {
		t.Fatalf("benchmarkExitCode() = %d, want 1", exitCode)
	}
}

func harnessTokenCount(input, output int) benchresult.TokenCount {
	return benchresult.TokenCount{Input: input, Output: output}
}
