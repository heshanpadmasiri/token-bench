package phoenix

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

func TestBenchmarkSpanNameNormalizesWhitespace(t *testing.T) {
	got := benchmarkSpanName("Go lang", "content\tbased\nrouter", "pi\u00a0agent")
	want := "benchmark-Go_lang-content_based_router-pi_agent"
	if got != want {
		t.Fatalf("benchmarkSpanName() = %q, want %q", got, want)
	}
}

func TestTracerBuildsOpenInferenceHierarchy(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := newTracer(Config{ProjectName: "trace-test", ExportTimeout: time.Second}, exporter, true)

	benchmark := tracer.StartBenchmarkSpan("go", "claim-check", "pi", "/tmp/skills")
	benchmark.AddModelUsage([]shared.ModelUsage{
		{Model: "model-b", Input: 2, Output: 3, CacheRead: 5, CacheWrite: 7},
		{Model: "model-a", Input: 11, Output: 13, CacheRead: 17, CacheWrite: 19},
	})
	benchmark.AddModelUsage([]shared.ModelUsage{{Model: "model-a", Input: 23, Output: 29, CacheRead: 31, CacheWrite: 37}})
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
	write.SetInput("go.mod", "module example\n\ngo 1.25\n")
	write.End()

	read := turn.StartReadToolSpan()
	read.SetInput("main.go", 2, 20)
	read.End()
	turn.End()
	benchmark.End()
	benchmark.End()

	spans := exporter.GetSpans()
	if len(spans) != 9 {
		t.Fatalf("got %d spans, want 9", len(spans))
	}
	byName := make(map[string]tracetest.SpanStub, len(spans))
	for _, current := range spans {
		byName[current.Name] = current
	}
	root := byName["benchmark-go-claim-check-pi"]
	llm := byName["llm"]
	if root.Parent.IsValid() {
		t.Fatalf("benchmark unexpectedly has parent %s", root.Parent.SpanID())
	}
	if llm.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Fatalf("LLM parent is not benchmark: root=%s parent=%s", root.SpanContext.SpanID(), llm.Parent.SpanID())
	}
	for _, current := range spans {
		if current.Status.Code != codes.Ok {
			t.Errorf("span %q status is %s, want OK", current.Name, current.Status.Code)
		}
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

	var usageModels []string
	usageByModel := make(map[string]tracetest.SpanStub)
	for _, current := range spans {
		if current.Name != "llm.usage" {
			continue
		}
		attributes := attributeMap(current.Attributes)
		model := attributes[llmModelName]
		usageModels = append(usageModels, model)
		usageByModel[model] = current
		if current.Parent.SpanID() != root.SpanContext.SpanID() {
			t.Errorf("usage for %q parent is %s, want benchmark %s", model, current.Parent.SpanID(), root.SpanContext.SpanID())
		}
	}
	if len(usageModels) != 2 || usageModels[0] != "model-a" || usageModels[1] != "model-b" {
		t.Fatalf("usage spans are not deterministic: %v", usageModels)
	}
	assertAttributes(t, usageByModel["model-a"].Attributes, map[string]string{
		openInferenceSpanKind: "LLM",
		llmModelName:          "model-a",
		llmTokenPrompt:        "138",
		llmTokenCompletion:    "42",
		llmTokenTotal:         "180",
		llmCacheRead:          "48",
		llmCacheWrite:         "56",
	})
	assertAttributes(t, usageByModel["model-b"].Attributes, map[string]string{
		llmTokenPrompt:     "14",
		llmTokenCompletion: "3",
		llmTokenTotal:      "17",
		llmCacheRead:       "5",
		llmCacheWrite:      "7",
	})

	assertAttributes(t, root.Attributes, map[string]string{
		openInferenceSpanKind:  "AGENT",
		"token_bench.language": "go",
		"token_bench.task":     "claim-check",
		"token_bench.harness":  "pi",
		"token_bench.skills":   "/tmp/skills",
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
		toolFilePath:  "main.go",
		inputValue:    "old_text:\nold\n\nnew_text:\nnew",
		inputMIMEType: textMIMEType,
	})
	assertAttributes(t, byName["write"].Attributes, map[string]string{
		toolFilePath:  "go.mod",
		inputValue:    "module example\n\ngo 1.25\n",
		inputMIMEType: textMIMEType,
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

func TestForceFlushExportsCompletedSpansBeforeShutdown(t *testing.T) {
	exporter := &retainingExporter{}
	tracer := newTracer(Config{ProjectName: "trace-test", ExportTimeout: time.Second}, exporter, false)

	benchmark := tracer.StartBenchmarkSpan("go", "task", "claude", "")
	turn := benchmark.StartTurn()
	tool := turn.StartBashToolSpan()
	tool.SetInput("go test ./...")
	tool.End()
	turn.End()
	benchmark.End()

	if err := tracer.provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exporter.getSpans()
	if len(spans) != 3 {
		t.Fatalf("got %d spans, want 3", len(spans))
	}
	byName := make(map[string]tracetest.SpanStub, len(spans))
	for _, current := range spans {
		byName[current.Name] = current
		if current.Status.Code != codes.Ok {
			t.Errorf("span %q status is %s, want OK", current.Name, current.Status.Code)
		}
	}
	root, llm, bash := byName["benchmark-go-task-claude"], byName["llm"], byName["bash"]
	if llm.Parent.SpanID() != root.SpanContext.SpanID() || bash.Parent.SpanID() != llm.SpanContext.SpanID() {
		t.Fatalf("spans do not preserve their hierarchy: %+v", spans)
	}
	if err := tracer.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownExportsFinalPartialBatchAndWaits(t *testing.T) {
	exporter := newBlockingExporter()
	tracer := newTracer(Config{ProjectName: "trace-test", ExportTimeout: time.Second}, exporter, false)
	tracer.StartBenchmarkSpan("go", "task", "claude", "").End()

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- tracer.Shutdown() }()
	awaitSignal(t, exporter.started, "export did not start during shutdown")
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before export finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(exporter.release)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not return after export finished")
	}
	if got := len(exporter.getSpans()); got != 1 {
		t.Fatalf("exported %d spans, want 1", got)
	}
}

func TestBatchQueueBlocksInsteadOfDroppingSpans(t *testing.T) {
	exporter := newBlockingExporter()
	tracer := newTracer(Config{ProjectName: "trace-test", ExportTimeout: 5 * time.Second}, exporter, false)

	for range 512 {
		tracer.StartBenchmarkSpan("go", "task", "claude", "").End()
	}
	awaitSignal(t, exporter.started, "full batch did not start exporting")
	for range 2048 {
		tracer.StartBenchmarkSpan("go", "task", "claude", "").End()
	}

	last := tracer.StartBenchmarkSpan("go", "task", "claude", "")
	endDone := make(chan struct{})
	go func() {
		last.End()
		close(endDone)
	}()
	select {
	case <-endDone:
		t.Fatal("End returned while the batch queue was full")
	case <-time.After(100 * time.Millisecond):
	}

	close(exporter.release)
	select {
	case <-endDone:
	case <-time.After(time.Second):
		t.Fatal("End remained blocked after queue space became available")
	}
	if err := tracer.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got, want := len(exporter.getSpans()), 2561; got != want {
		t.Fatalf("exported %d spans, want %d", got, want)
	}
}

func TestSpanErrorStatusIsExported(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := newTracer(Config{ProjectName: "trace-test", ExportTimeout: time.Second}, exporter, true)
	benchmark := tracer.StartBenchmarkSpan("go", "task", "claude", "")
	benchmark.SetError("validation failed")
	benchmark.End()

	span := exporter.GetSpans()[0]
	if span.Status.Code != codes.Error || span.Status.Description != "validation failed" {
		t.Fatalf("unexpected status: %+v", span.Status)
	}
	if len(span.Events) != 1 || span.Events[0].Name != "exception" {
		t.Fatalf("error event was not recorded: %+v", span.Events)
	}
}

func TestPanicExporterPanicsWhenExportFails(t *testing.T) {
	expected := errors.New("Phoenix unavailable")
	exporter := panicExporter{delegate: failingExporter{exportErr: expected}}
	assertPanicsWith(t, expected, func() {
		_ = exporter.ExportSpans(context.Background(), nil)
	})
}

func TestPanicExporterPanicsWhenShutdownFails(t *testing.T) {
	expected := errors.New("Phoenix shutdown failed")
	exporter := panicExporter{delegate: failingExporter{shutdownErr: expected}}
	assertPanicsWith(t, expected, func() {
		_ = exporter.Shutdown(context.Background())
	})
}

func assertPanicsWith(t *testing.T, expected error, action func()) {
	t.Helper()
	defer func() {
		recovered := recover()
		err, ok := recovered.(error)
		if !ok || !errors.Is(err, expected) {
			t.Fatalf("panic %v does not contain %v", recovered, expected)
		}
	}()
	action()
	t.Fatal("action did not panic")
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
		result[string(item.Key)] = item.Value.Emit()
	}
	return result
}

type retainingExporter struct {
	mu    sync.Mutex
	spans tracetest.SpanStubs
}

func (e *retainingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, tracetest.SpanStubsFromReadOnlySpans(spans)...)
	return nil
}

func (*retainingExporter) Shutdown(context.Context) error { return nil }

func (e *retainingExporter) getSpans() tracetest.SpanStubs {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append(tracetest.SpanStubs(nil), e.spans...)
}

type blockingExporter struct {
	retainingExporter
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingExporter() *blockingExporter {
	return &blockingExporter{started: make(chan struct{}), release: make(chan struct{})}
}

func (e *blockingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return e.retainingExporter.ExportSpans(ctx, spans)
}

func awaitSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}

type failingExporter struct {
	exportErr   error
	shutdownErr error
}

func (e failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return e.exportErr
}

func (e failingExporter) Shutdown(context.Context) error {
	return e.shutdownErr
}
