package phoenix

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	"token-bench/tracing/internal/shared"
)

type span struct {
	span oteltrace.Span
	once sync.Once
}

func (s *span) End() {
	s.once.Do(func() { s.span.End() })
}

type benchmarkSpan struct {
	span
	context context.Context
	tracer  oteltrace.Tracer
}

func (b *benchmarkSpan) StartTurn() shared.TurnSpan {
	ctx, child := b.tracer.Start(b.context, "llm", oteltrace.WithAttributes(
		attribute.String(openInferenceSpanKind, "LLM"),
	))
	return &turnSpan{span: span{span: child}, context: ctx, tracer: b.tracer}
}

type turnSpan struct {
	span
	context context.Context
	tracer  oteltrace.Tracer
}

func (t *turnSpan) SetLLMInput(value string) {
	setTextInput(t.span.span, value)
}

func (t *turnSpan) SetLLMOutput(value string) {
	setTextOutput(t.span.span, value)
}

func (t *turnSpan) StartGenericToolSpan(name string) shared.GenericToolSpan {
	return &genericToolSpan{toolSpan: t.startTool(name)}
}

func (t *turnSpan) StartBashToolSpan() shared.BashToolSpan {
	return &bashToolSpan{toolSpan: t.startTool("bash")}
}

func (t *turnSpan) StartEditToolSpan() shared.EditToolSpan {
	return &editToolSpan{toolSpan: t.startTool("edit")}
}

func (t *turnSpan) StartWriteToolSpan() shared.WriteToolSpan {
	return &writeToolSpan{toolSpan: t.startTool("write")}
}

func (t *turnSpan) StartReadToolSpan() shared.ReadToolSpan {
	return &readToolSpan{toolSpan: t.startTool("read")}
}

func (t *turnSpan) startTool(name string) toolSpan {
	_, child := t.tracer.Start(t.context, name, oteltrace.WithAttributes(
		attribute.String(openInferenceSpanKind, "TOOL"),
		attribute.String(toolName, name),
	))
	return toolSpan{span: span{span: child}}
}

type toolSpan struct {
	span
}

func (t *toolSpan) SetOutput(value string) {
	setTextOutput(t.span.span, value)
}

type genericToolSpan struct {
	toolSpan
}

func (t *genericToolSpan) SetInput(value string) {
	setTextInput(t.span.span, value)
}

type bashToolSpan struct {
	toolSpan
}

func (b *bashToolSpan) SetInput(command string) {
	setJSONInput(b.span.span, struct {
		Command string `json:"command"`
	}{Command: command})
}

type editToolSpan struct {
	toolSpan
}

func (e *editToolSpan) SetInput(path string, edits []shared.Edit) {
	type serializedEdit struct {
		OldText string `json:"old_text"`
		NewText string `json:"new_text"`
	}
	serialized := make([]serializedEdit, len(edits))
	for index, edit := range edits {
		serialized[index] = serializedEdit{OldText: edit.OldText, NewText: edit.NewText}
	}
	setJSONInput(e.span.span, struct {
		Path  string           `json:"path"`
		Edits []serializedEdit `json:"edits"`
	}{Path: path, Edits: serialized})
}

type writeToolSpan struct {
	toolSpan
}

func (w *writeToolSpan) SetInput(path, content string) {
	setJSONInput(w.span.span, struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}{Path: path, Content: content})
}

type readToolSpan struct {
	toolSpan
}

func (r *readToolSpan) SetInput(path string, offset, limit int) {
	setJSONInput(r.span.span, struct {
		Path   string `json:"path"`
		Offset int    `json:"offset"`
		Limit  int    `json:"limit"`
	}{Path: path, Offset: offset, Limit: limit})
}
