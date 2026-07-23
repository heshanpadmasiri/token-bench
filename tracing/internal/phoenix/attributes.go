package phoenix

import (
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	openInferenceSpanKind = "openinference.span.kind"
	openInferenceProject  = "openinference.project.name"
	inputValue            = "input.value"
	inputMIMEType         = "input.mime_type"
	outputValue           = "output.value"
	outputMIMEType        = "output.mime_type"
	toolName              = "tool.name"
	toolFilePath          = "tool.file_path"
	textMIMEType          = "text/plain"
	jsonMIMEType          = "application/json"
)

func setTextInput(span oteltrace.Span, value string) {
	span.SetAttributes(
		attribute.String(inputValue, value),
		attribute.String(inputMIMEType, textMIMEType),
	)
}

func setJSONInput(span oteltrace.Span, value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		// All values passed by this package consist only of strings and integers,
		// so this fallback is defensive rather than an expected path.
		setTextInput(span, err.Error())
		return
	}
	span.SetAttributes(
		attribute.String(inputValue, string(encoded)),
		attribute.String(inputMIMEType, jsonMIMEType),
	)
}

func setTextOutput(span oteltrace.Span, value string) {
	span.SetAttributes(
		attribute.String(outputValue, value),
		attribute.String(outputMIMEType, textMIMEType),
	)
}
