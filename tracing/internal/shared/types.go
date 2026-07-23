// Package shared contains the backend-independent tracing contracts.
package shared

type (
	// Tracer creates benchmark traces and owns the exporter lifecycle.
	// The component that creates a Tracer must call Shutdown after ending all spans.
	Tracer interface {
		StartBenchmarkSpan(task, harness, skills string) BenchmarkSpan
		Shutdown() error
	}

	// Span is a unit of traced work.
	Span interface {
		// SetOK marks the span successful.
		SetOK()
		// SetError marks the span failed and records a description.
		SetError(description string)
		End()
	}

	// BenchmarkSpan is the root span for one benchmark run.
	BenchmarkSpan interface {
		Span
		StartTurn() TurnSpan
		// AddModelUsage adds benchmark usage grouped by concrete model identifier.
		AddModelUsage([]ModelUsage)
	}

	// ModelUsage contains non-overlapping token usage dimensions for one model.
	ModelUsage struct {
		Model      string
		Input      int
		Output     int
		CacheRead  int
		CacheWrite int
	}

	// TurnSpan records one model turn and its tool calls.
	TurnSpan interface {
		Span
		SetLLMInput(string)
		SetLLMOutput(string)
		StartGenericToolSpan(name string) GenericToolSpan
		StartBashToolSpan() BashToolSpan
		StartEditToolSpan() EditToolSpan
		StartWriteToolSpan() WriteToolSpan
		StartReadToolSpan() ReadToolSpan
	}

	// ToolSpan records output common to all tools.
	ToolSpan interface {
		Span
		SetOutput(string)
	}

	// GenericToolSpan records an arbitrary tool's input as text.
	GenericToolSpan interface {
		ToolSpan
		SetInput(string)
	}

	// BashToolSpan records a shell command invocation.
	BashToolSpan interface {
		GenericToolSpan
	}

	// EditToolSpan records edits to one file.
	EditToolSpan interface {
		ToolSpan
		SetInput(path string, edits []Edit)
	}

	// Edit describes one exact text replacement.
	Edit struct {
		OldText string
		NewText string
	}

	// WriteToolSpan records a complete file write.
	WriteToolSpan interface {
		ToolSpan
		SetInput(path, content string)
	}

	// ReadToolSpan records a file read range.
	ReadToolSpan interface {
		ToolSpan
		SetInput(path string, offset, limit int)
	}
)
