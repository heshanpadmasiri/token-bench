package main

import (
	"testing"

	benchresult "token-bench/result"
)

func TestParseOptions(t *testing.T) {
	parsed, err := parseOptions([]string{"content-based-router", "ballerina", "pi", "--n-runs=3", "--target=/tmp/token-bench"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.runs != 3 || parsed.target != "/tmp/token-bench" {
		t.Fatalf("unexpected options: %+v", parsed)
	}
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
