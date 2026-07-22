package result

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestWriteRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	want := Result{
		SchemaVersion:  SchemaVersion,
		Task:           "router",
		Language:       "go",
		Harness:        "pi",
		SkillsDir:      "/tmp/benchmark-skills",
		PromptTemplate: "benchmark prompt template",
		Run:            1,
		RequestedRuns:  2,
		Passed:         true,
		Tokens:         TokenCount{Input: 10, Output: 20, Total: 30},
	}
	if err := Write(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestValidateRejectsUnknownSchema(t *testing.T) {
	value := Result{SchemaVersion: 1, Task: "router", Language: "go", Harness: "pi", Run: 1, RequestedRuns: 1}
	if err := value.Validate(); err == nil {
		t.Fatal("expected schema validation error")
	}
}

func TestValidateRejectsRelativeSkillsDirectory(t *testing.T) {
	value := Result{SchemaVersion: SchemaVersion, Task: "router", Language: "go", Harness: "pi", SkillsDir: "skills", Run: 1, RequestedRuns: 1}
	if err := value.Validate(); err == nil {
		t.Fatal("expected skills directory validation error")
	}
}
