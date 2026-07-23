package phoenix

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"token-bench/tracing/internal/shared"
)

func TestNormalizeConfig(t *testing.T) {
	headers := map[string]string{"Authorization": "Bearer secret"}
	config, err := normalizeConfig(Config{Endpoint: " http://localhost:6006/ ", Headers: headers})
	if err != nil {
		t.Fatal(err)
	}
	if config.Endpoint != "http://localhost:6006/v1/traces" {
		t.Fatalf("unexpected endpoint %q", config.Endpoint)
	}
	if config.ProjectName != defaultProjectName || config.ExportTimeout != defaultExportTimeout {
		t.Fatalf("unexpected defaults: %+v", config)
	}
	headers["Authorization"] = "changed"
	if config.Headers["Authorization"] != "Bearer secret" {
		t.Fatal("configuration retained the caller's mutable headers map")
	}

	full, err := normalizeConfig(Config{Endpoint: "https://phoenix.example/prefix/v1/traces", ExportTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if full.Endpoint != "https://phoenix.example/prefix/v1/traces" {
		t.Fatalf("full trace endpoint changed to %q", full.Endpoint)
	}
}

func TestNormalizeConfigRejectsInvalidValues(t *testing.T) {
	for _, config := range []Config{
		{},
		{Endpoint: "localhost:6006"},
		{Endpoint: "ftp://localhost:6006"},
		{Endpoint: "http://localhost:6006?project=test"},
		{Endpoint: "http://localhost:6006", ExportTimeout: -time.Second},
	} {
		if _, err := normalizeConfig(config); err == nil {
			t.Errorf("normalizeConfig(%+v) unexpectedly succeeded", config)
		}
	}
}

func TestTracerBuildsOpenInferenceHierarchy(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := newTracer(Config{ProjectName: "trace-test", ExportTimeout: time.Second}, exporter, true)

	benchmark := tracer.StartBenchmarkSpan("claim-check", "pi", "/tmp/skills")
	turn := benchmark.StartTurn()
	turn.SetLLMInput("build the server")
	turn.SetLLMOutput("done")

	generic := turn.StartGenericToolSpan("search")
	generic.SetInput("find handlers")
	generic.SetOutput("handler.go")
	generic.End()

	bash := turn.StartBashToolSpan()
	bash.SetInput("go test ./...")
	bash.SetOutput("ok")
	bash.End()

	edit := turn.StartEditToolSpan()
	edit.SetInput("main.go", []shared.Edit{{OldText: "old", NewText: "new"}})
	edit.End()

	write := turn.StartWriteToolSpan()
	write.SetInput("go.mod", "module example")
	write.End()

	read := turn.StartReadToolSpan()
	read.SetInput("main.go", 2, 20)
	read.End()
	turn.End()
	benchmark.End()

	spans := exporter.GetSpans()
	if len(spans) != 7 {
		t.Fatalf("got %d spans, want 7", len(spans))
	}
	byName := make(map[string]tracetest.SpanStub, len(spans))
	for _, current := range spans {
		byName[current.Name] = current
	}
	root := byName["benchmark"]
	llm := byName["llm"]
	if root.Parent.IsValid() {
		t.Fatalf("benchmark unexpectedly has parent %s", root.Parent.SpanID())
	}
	if llm.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Fatalf("LLM parent is not benchmark: root=%s parent=%s", root.SpanContext.SpanID(), llm.Parent.SpanID())
	}
	for _, name := range []string{"search", "bash", "edit", "write", "read"} {
		current, ok := byName[name]
		if !ok {
			t.Errorf("missing %q span", name)
			continue
		}
		if current.Parent.SpanID() != llm.SpanContext.SpanID() {
			t.Errorf("%q parent is %s, want LLM %s", name, current.Parent.SpanID(), llm.SpanContext.SpanID())
		}
	}

	assertAttributes(t, root.Attributes, map[string]string{
		openInferenceSpanKind: "AGENT",
		"token_bench.task":    "claim-check",
		"token_bench.harness": "pi",
		"token_bench.skills":  "/tmp/skills",
	})
	assertAttributes(t, llm.Attributes, map[string]string{
		openInferenceSpanKind: "LLM",
		inputValue:            "build the server",
		inputMIMEType:         textMIMEType,
		outputValue:           "done",
		outputMIMEType:        textMIMEType,
	})
	assertAttributes(t, byName["bash"].Attributes, map[string]string{
		openInferenceSpanKind: "TOOL",
		toolName:              "bash",
		inputValue:            `{"command":"go test ./..."}`,
		inputMIMEType:         jsonMIMEType,
		outputValue:           "ok",
	})
	assertAttributes(t, byName["edit"].Attributes, map[string]string{
		inputValue:    `{"path":"main.go","edits":[{"old_text":"old","new_text":"new"}]}`,
		inputMIMEType: jsonMIMEType,
	})
	assertAttributes(t, byName["write"].Attributes, map[string]string{
		inputValue: `{"path":"go.mod","content":"module example"}`,
	})
	assertAttributes(t, byName["read"].Attributes, map[string]string{
		inputValue: `{"path":"main.go","offset":2,"limit":20}`,
	})

	resourceAttributes := attributeMap(root.Resource.Attributes())
	if resourceAttributes["service.name"] != "trace-test" || resourceAttributes[openInferenceProject] != "trace-test" {
		t.Fatalf("unexpected resource attributes: %v", resourceAttributes)
	}
	if err := tracer.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := tracer.Shutdown(); err != nil {
		t.Fatalf("second shutdown failed: %v", err)
	}
}

func TestShutdownReturnsExportFailures(t *testing.T) {
	expected := errors.New("Phoenix unavailable")
	tracer := newTracer(Config{ProjectName: "test", ExportTimeout: time.Second}, failingExporter{err: expected}, true)
	span := tracer.StartBenchmarkSpan("task", "harness", "")
	span.End()
	if err := tracer.Shutdown(); !errors.Is(err, expected) {
		t.Fatalf("Shutdown error %v does not contain %v", err, expected)
	}
}

func assertAttributes(t *testing.T, attributes []attribute.KeyValue, expected map[string]string) {
	t.Helper()
	actual := attributeMap(attributes)
	for name, value := range expected {
		if actual[name] != value {
			t.Errorf("attribute %q = %q, want %q; all attributes: %v", name, actual[name], value, actual)
		}
	}
}

func attributeMap(attributes []attribute.KeyValue) map[string]string {
	result := make(map[string]string, len(attributes))
	for _, item := range attributes {
		result[string(item.Key)] = item.Value.AsString()
	}
	return result
}

type failingExporter struct {
	err error
}

func (e failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return e.err
}

func (failingExporter) Shutdown(context.Context) error {
	return nil
}
