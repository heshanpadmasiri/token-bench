package main

import (
	"testing"

	"token-bench/harness"
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

func harnessTokenCount(input, output int) harness.TokenCount {
	return harness.TokenCount{Input: input, Output: output}
}
