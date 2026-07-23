// Package phoenix implements tracing export to Arize Phoenix.
package phoenix

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	"token-bench/tracing/internal/shared"
)

const (
	defaultProjectName   = "token-bench"
	defaultExportTimeout = 10 * time.Second
	instrumentationName  = "token-bench/tracing/phoenix"
)

// Config contains Phoenix exporter configuration supplied by the public package.
type Config struct {
	Endpoint      string
	ProjectName   string
	Headers       map[string]string
	ExportTimeout time.Duration
}

type tracer struct {
	tracer          oteltrace.Tracer
	provider        *sdktrace.TracerProvider
	exporter        *recordingExporter
	exportTimeout   time.Duration
	shutdownOnce    sync.Once
	shutdownFailure error
}

// New creates an OTLP/HTTP tracer configured for Phoenix.
func New(config Config) (shared.Tracer, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), normalized.ExportTimeout)
	defer cancel()
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(normalized.Endpoint),
		otlptracehttp.WithHeaders(normalized.Headers),
		otlptracehttp.WithTimeout(normalized.ExportTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize Phoenix OTLP exporter: %w", err)
	}
	return newTracer(normalized, exporter, false), nil
}

func newTracer(config Config, exporter sdktrace.SpanExporter, synchronous bool) *tracer {
	recording := &recordingExporter{delegate: exporter, deferred: !synchronous}
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(config.ProjectName),
		attribute.String(openInferenceProject, config.ProjectName),
	)
	var processorOption sdktrace.TracerProviderOption
	if synchronous {
		processorOption = sdktrace.WithSyncer(recording)
	} else {
		processorOption = sdktrace.WithBatcher(recording, sdktrace.WithExportTimeout(config.ExportTimeout))
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), processorOption)
	return &tracer{
		tracer:        provider.Tracer(instrumentationName),
		provider:      provider,
		exporter:      recording,
		exportTimeout: config.ExportTimeout,
	}
}

func (t *tracer) StartBenchmarkSpan(task, harness, skills string) shared.BenchmarkSpan {
	ctx, root := t.tracer.Start(context.Background(), "benchmark", oteltrace.WithAttributes(
		attribute.String(openInferenceSpanKind, "AGENT"),
		attribute.String("token_bench.task", task),
		attribute.String("token_bench.harness", harness),
		attribute.String("token_bench.skills", skills),
	))
	return &benchmarkSpan{span: span{span: root}, context: ctx, tracer: t.tracer}
}

func (t *tracer) Shutdown() error {
	t.shutdownOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), t.exportTimeout)
		defer cancel()
		t.shutdownFailure = errors.Join(t.provider.Shutdown(ctx), t.exporter.exportError())
	})
	return t.shutdownFailure
}

func normalizeConfig(config Config) (Config, error) {
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	if config.Endpoint == "" {
		return Config{}, errors.New("Phoenix endpoint is required")
	}
	parsed, err := url.Parse(config.Endpoint)
	if err != nil {
		return Config{}, fmt.Errorf("parse Phoenix endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Config{}, fmt.Errorf("Phoenix endpoint scheme must be http or https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return Config{}, errors.New("Phoenix endpoint must include a host")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return Config{}, errors.New("Phoenix endpoint must not include a query or fragment")
	}
	path := strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(path, "/v1/traces") {
		path += "/v1/traces"
	}
	parsed.Path = path
	config.Endpoint = parsed.String()

	config.ProjectName = strings.TrimSpace(config.ProjectName)
	if config.ProjectName == "" {
		config.ProjectName = defaultProjectName
	}
	if config.ExportTimeout < 0 {
		return Config{}, errors.New("Phoenix export timeout must not be negative")
	}
	if config.ExportTimeout == 0 {
		config.ExportTimeout = defaultExportTimeout
	}
	config.Headers = cloneHeaders(config.Headers)
	return config, nil
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers))
	for name, value := range headers {
		cloned[name] = value
	}
	return cloned
}

type recordingExporter struct {
	delegate sdktrace.SpanExporter
	deferred bool
	mu       sync.Mutex
	pending  []sdktrace.ReadOnlySpan
	failures []error
}

func (e *recordingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e.deferred {
		e.mu.Lock()
		e.pending = append(e.pending, spans...)
		e.mu.Unlock()
		return nil
	}
	return e.export(ctx, spans)
}

func (e *recordingExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	pending := append([]sdktrace.ReadOnlySpan(nil), e.pending...)
	e.pending = nil
	e.mu.Unlock()

	var exportErr error
	if len(pending) != 0 {
		sortParentFirst(pending)
		exportErr = e.export(ctx, pending)
	}
	return errors.Join(exportErr, e.delegate.Shutdown(ctx))
}

func (e *recordingExporter) export(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.delegate.ExportSpans(ctx, spans)
	if err != nil {
		e.mu.Lock()
		e.failures = append(e.failures, err)
		e.mu.Unlock()
	}
	return err
}

func sortParentFirst(spans []sdktrace.ReadOnlySpan) {
	byID := make(map[oteltrace.SpanID]sdktrace.ReadOnlySpan, len(spans))
	for _, current := range spans {
		byID[current.SpanContext().SpanID()] = current
	}
	depths := make(map[oteltrace.SpanID]int, len(spans))
	var depth func(sdktrace.ReadOnlySpan) int
	depth = func(current sdktrace.ReadOnlySpan) int {
		id := current.SpanContext().SpanID()
		if value, ok := depths[id]; ok {
			return value
		}
		parent, ok := byID[current.Parent().SpanID()]
		if !ok {
			depths[id] = 0
			return 0
		}
		value := depth(parent) + 1
		depths[id] = value
		return value
	}
	sort.SliceStable(spans, func(left, right int) bool {
		leftDepth, rightDepth := depth(spans[left]), depth(spans[right])
		if leftDepth != rightDepth {
			return leftDepth < rightDepth
		}
		return spans[left].StartTime().Before(spans[right].StartTime())
	})
}

func (e *recordingExporter) exportError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return errors.Join(e.failures...)
}
