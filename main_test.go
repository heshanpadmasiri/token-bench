package main

import (
	"os"
	"path/filepath"
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

func TestSelectLanguage(t *testing.T) {
	for _, name := range []string{"ballerina", "go"} {
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
