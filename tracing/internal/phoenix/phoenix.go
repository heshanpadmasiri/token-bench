// Package phoenix implements tracing export to Arize Phoenix.
package phoenix

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
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
	defaultProjectName    = "token-bench"
	defaultExportTimeout  = 10 * time.Second
	defaultExportAttempts = 3
	defaultRetryDelay     = time.Second
	instrumentationName   = "token-bench/tracing/phoenix"
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
		// reportingExporter owns retries so failures such as a bare EOF, which
		// the OTLP exporter does not classify as retryable, are also retried.
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize Phoenix OTLP exporter: %w", err)
	}
	return newTracer(normalized, exporter, false), nil
}

func newTracer(config Config, exporter sdktrace.SpanExporter, synchronous bool) *tracer {
	wrapped := &reportingExporter{
		delegate:    exporter,
		maxAttempts: defaultExportAttempts,
		retryDelay:  defaultRetryDelay,
	}
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

// reportingExporter retries failed exports and retains the final error so it can
// be reported during shutdown without crashing the batch processor goroutine.
type reportingExporter struct {
	delegate    sdktrace.SpanExporter
	maxAttempts int
	retryDelay  time.Duration

	mu        sync.Mutex
	exportErr error
}

func (e *reportingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	attempts := max(1, e.maxAttempts)
	attempted := 0
	var exportErr error
retry:
	for attempt := range attempts {
		attempted++
		exportErr = e.delegate.ExportSpans(ctx, spans)
		if exportErr == nil {
			return nil
		}
		if attempt+1 == attempts || ctx.Err() != nil {
			break
		}
		delay := e.retryDelay * time.Duration(1<<attempt)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			exportErr = errors.Join(exportErr, ctx.Err())
			break retry
		}
	}

	e.mu.Lock()
	if e.exportErr == nil {
		e.exportErr = fmt.Errorf("export traces after %d attempts: %w", attempted, exportErr)
	}
	e.mu.Unlock()
	// The batch processor cannot return asynchronous errors to its caller. Keep
	// the error for Shutdown instead of invoking OpenTelemetry's global handler.
	return nil
}

func (e *reportingExporter) Shutdown(ctx context.Context) error {
	shutdownErr := e.delegate.Shutdown(ctx)
	e.mu.Lock()
	defer e.mu.Unlock()
	return errors.Join(e.exportErr, shutdownErr)
}
