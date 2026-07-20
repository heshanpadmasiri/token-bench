package main

import "testing"

func TestParseOptionsAllowsOutputAfterPaths(t *testing.T) {
	parsed, err := parseOptions([]string{"first", "second", "--output", "custom.html"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.output != "custom.html" || len(parsed.paths) != 2 {
		t.Fatalf("unexpected options: %+v", parsed)
	}
}

func TestParseOptionsDefaultsOutput(t *testing.T) {
	parsed, err := parseOptions([]string{"results"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.output != "report.html" {
		t.Fatalf("unexpected output: %s", parsed.output)
	}
}
