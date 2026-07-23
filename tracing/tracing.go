// Package tracing provides backend-independent agent harness tracing.
package tracing

import (
	"time"

	"token-bench/tracing/internal/phoenix"
	"token-bench/tracing/internal/shared"
)

type (
	Tracer          = shared.Tracer
	Span            = shared.Span
	BenchmarkSpan   = shared.BenchmarkSpan
	TurnSpan        = shared.TurnSpan
	ToolSpan        = shared.ToolSpan
	GenericToolSpan = shared.GenericToolSpan
	BashToolSpan    = shared.BashToolSpan
	EditToolSpan    = shared.EditToolSpan
	Edit            = shared.Edit
	WriteToolSpan   = shared.WriteToolSpan
	ReadToolSpan    = shared.ReadToolSpan

	// PhoenixConfig configures OTLP/HTTP trace export to Arize Phoenix.
	PhoenixConfig struct {
		// Endpoint may be the Phoenix base URL or the full /v1/traces URL.
		Endpoint string
		// ProjectName is the project shown in Phoenix. Empty defaults to token-bench.
		ProjectName string
		// Headers contains optional OTLP request headers, such as Authorization.
		Headers map[string]string
		// ExportTimeout bounds exports and Shutdown. Zero defaults to ten seconds.
		ExportTimeout time.Duration
	}
)

// NewPhoenix creates a tracer that exports OTLP/HTTP protobuf spans to Phoenix.
// The caller owns the returned tracer and must call Shutdown after ending all spans.
func NewPhoenix(config PhoenixConfig) (Tracer, error) {
	return phoenix.New(phoenix.Config{
		Endpoint:      config.Endpoint,
		ProjectName:   config.ProjectName,
		Headers:       config.Headers,
		ExportTimeout: config.ExportTimeout,
	})
}
