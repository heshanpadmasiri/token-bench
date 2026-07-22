package report

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"token-bench/result"
)

func TestGenerateDiscoversNestedResultsAndExcludesFailures(t *testing.T) {
	root := t.TempDir()
	writeTestResult(t, filepath.Join(root, "go", "run-001", "result.json"), result.Result{
		SchemaVersion: result.SchemaVersion, Task: "router", Language: "go", Harness: "pi",
		Run: 1, RequestedRuns: 2, Passed: true, Tokens: result.TokenCount{Input: 100, Output: 200, Total: 300},
	})
	writeTestResult(t, filepath.Join(root, "go", "run-002", "result.json"), result.Result{
		SchemaVersion: result.SchemaVersion, Task: "router", Language: "go", Harness: "pi",
		Run: 2, RequestedRuns: 2, Tokens: result.TokenCount{Input: 999, Output: 999, Total: 1998},
	})

	var output bytes.Buffer
	if err := Generate([]string{root}, &output); err != nil {
		t.Fatal(err)
	}
	html := output.String()
	for _, expected := range []string{"router", "go", "Benchmark prompt template", "test prompt template", "1 of 2 runs failed", "300.0", "100.0", "200.0", "50.0%", "mean ± 1σ"} {
		if !strings.Contains(html, expected) {
			t.Errorf("report does not contain %q", expected)
		}
	}
	if strings.Contains(html, "1998.0") {
		t.Fatal("failed run was included in averages")
	}
}

func TestGenerateComparesSkillsConfigurations(t *testing.T) {
	root := t.TempDir()
	skills := filepath.Join(root, "skills")
	for index, skillsDir := range []string{"", skills} {
		writeTestResult(t, filepath.Join(root, "variant", string(rune('a'+index)), "result.json"), result.Result{
			SchemaVersion: result.SchemaVersion, Task: "router", Language: "go", Harness: "pi", SkillsDir: skillsDir,
			Run: 1, RequestedRuns: 1, Passed: true, Tokens: result.TokenCount{Total: 100 + index},
		})
	}

	var output bytes.Buffer
	if err := Generate([]string{root}, &output); err != nil {
		t.Fatal(err)
	}
	html := output.String()
	for _, expected := range []string{"No skills", skills, "2 language/skills variants"} {
		if !strings.Contains(html, expected) {
			t.Errorf("report does not contain %q", expected)
		}
	}
}

func TestDistribution(t *testing.T) {
	minimum, q1, median, q3, maximum, mean, deviation := distribution([]float64{10, 20, 30})
	if minimum != 10 || q1 != 15 || median != 20 || q3 != 25 || maximum != 30 || mean != 20 {
		t.Fatalf("unexpected distribution: %v %v %v %v %v %v", minimum, q1, median, q3, maximum, mean)
	}
	if math.Abs(deviation-math.Sqrt(200.0/3.0)) > 0.0001 {
		t.Fatalf("unexpected deviation: %v", deviation)
	}
}

func TestGenerateRejectsMultipleHarnesses(t *testing.T) {
	root := t.TempDir()
	for _, harness := range []string{"pi", "other"} {
		writeTestResult(t, filepath.Join(root, harness, "result.json"), result.Result{
			SchemaVersion: result.SchemaVersion, Task: "router", Language: "go", Harness: harness,
			Run: 1, RequestedRuns: 1, Passed: true,
		})
	}
	var output bytes.Buffer
	err := Generate([]string{root}, &output)
	if err == nil || !strings.Contains(err.Error(), "multiple harnesses") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateUsesEmbeddedTemplateForLegacyResults(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "result.json")
	value := result.Result{
		SchemaVersion: result.SchemaVersion, Task: "content-based-router", Language: "go", Harness: "pi",
		Run: 1, RequestedRuns: 1, Passed: true,
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := result.Write(path, value); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := Generate([]string{root}, &output); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"Benchmark prompt template", "Implement the Content-Based Router", "{{index .Ports 0}}"} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("report does not contain %q", expected)
		}
	}
}

func TestGenerateRejectsDifferentPromptTemplatesForTask(t *testing.T) {
	root := t.TempDir()
	for index, promptTemplate := range []string{"first", "second"} {
		writeTestResult(t, filepath.Join(root, string(rune('a'+index)), "result.json"), result.Result{
			SchemaVersion: result.SchemaVersion, Task: "router", Language: "go", Harness: "pi",
			PromptTemplate: promptTemplate, Run: 1, RequestedRuns: 1, Passed: true,
		})
	}
	var output bytes.Buffer
	err := Generate([]string{root}, &output)
	if err == nil || !strings.Contains(err.Error(), "multiple benchmark prompt templates") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func writeTestResult(t *testing.T, path string, value result.Result) {
	t.Helper()
	if value.PromptTemplate == "" {
		value.PromptTemplate = "test prompt template"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := result.Write(path, value); err != nil {
		t.Fatal(err)
	}
}
