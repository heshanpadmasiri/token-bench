package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"token-bench/harness"
	benchresult "token-bench/result"
)

func TestParseOptions(t *testing.T) {
	parsed, err := parseOptions([]string{"content-based-router", "ballerina", "pi", "--n-runs=3", "--target=/tmp/token-bench", "--skills=/tmp/skills"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.runs != 3 || parsed.target != "/tmp/token-bench" || parsed.skillsDir != "/tmp/skills" {
		t.Fatalf("unexpected options: %+v", parsed)
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
	for _, name := range []string{"content-based-router", "dead-letter-channel", "scatter-gather"} {
		if _, err := selectTask(name); err != nil {
			t.Errorf("selectTask(%q): %v", name, err)
		}
	}
	if _, err := selectTask("unknown"); err == nil {
		t.Fatal("expected unknown task error")
	}
}

func TestSelectLanguage(t *testing.T) {
	for _, name := range []string{"ballerina", "go", "java"} {
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

func harnessTokenCount(input, output int) benchresult.TokenCount {
	return benchresult.TokenCount{Input: input, Output: output}
}
