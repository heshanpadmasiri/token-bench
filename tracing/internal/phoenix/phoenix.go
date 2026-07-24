// Package phoenix implements tracing export to Arize Phoenix.
package phoenix

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

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
	tracer   oteltrace.Tracer
	provider *sdktrace.TracerProvider
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
	wrapped := panicExporter{delegate: exporter}
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(config.ProjectName),
		attribute.String(openInferenceProject, config.ProjectName),
	)
	var processorOption sdktrace.TracerProviderOption
	if synchronous {
		processorOption = sdktrace.WithSyncer(wrapped)
	} else {
		processorOption = sdktrace.WithBatcher(
			wrapped,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithMaxExportBatchSize(512),
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithExportTimeout(config.ExportTimeout),
			sdktrace.WithBlocking(),
		)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), processorOption)
	return &tracer{
		tracer:   provider.Tracer(instrumentationName),
		provider: provider,
	}
}

func (t *tracer) StartBenchmarkSpan(language, task, harness, skills string) shared.BenchmarkSpan {
	ctx, root := t.tracer.Start(context.Background(), benchmarkSpanName(language, task, harness), oteltrace.WithAttributes(
		attribute.String(openInferenceSpanKind, "AGENT"),
		attribute.String("token_bench.language", language),
		attribute.String("token_bench.task", task),
		attribute.String("token_bench.harness", harness),
		attribute.String("token_bench.skills", skills),
	))
	return &benchmarkSpan{span: span{span: root}, context: ctx, tracer: t.tracer}
}

func benchmarkSpanName(language, task, harness string) string {
	normalize := func(value string) string {
		return strings.Map(func(character rune) rune {
			if unicode.IsSpace(character) {
				return '_'
			}
			return character
		}, value)
	}
	return fmt.Sprintf("benchmark-%s-%s-%s", normalize(language), normalize(task), normalize(harness))
}

func (t *tracer) Shutdown() error {
	if err := t.provider.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	return nil
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

type panicExporter struct {
	delegate sdktrace.SpanExporter
}

func (e panicExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.delegate.ExportSpans(ctx, spans); err != nil {
		panic(err)
	}
	return nil
}

func (e panicExporter) Shutdown(ctx context.Context) error {
	if err := e.delegate.Shutdown(ctx); err != nil {
		panic(err)
	}
	return nil
}
