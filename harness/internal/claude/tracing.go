package claude

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"token-bench/tracing"
)

// traceAdapter converts Claude's stream-json protocol into backend-independent spans.
type traceAdapter struct {
	mu        sync.Mutex
	benchmark tracing.BenchmarkSpan
	turn      tracing.TurnSpan
	tools     map[string]tracing.ToolSpan
	nextInput []string
	closed    bool
}

func newTraceAdapter(benchmark tracing.BenchmarkSpan) *traceAdapter {
	return &traceAdapter{benchmark: benchmark, tools: make(map[string]tracing.ToolSpan)}
}

func (a *traceAdapter) SetNextInput(input string) {
	if a == nil || a.benchmark == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		a.nextInput = append(a.nextInput, input)
	}
}

func (a *traceAdapter) Process(eventType string, raw json.RawMessage) error {
	if a == nil || a.benchmark == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}

	switch eventType {
	case "assistant":
		return a.processAssistant(raw)
	case "user":
		return a.processUser(raw)
	case "result":
		var event struct {
			Subtype string   `json:"subtype"`
			IsError bool     `json:"is_error"`
			Errors  []string `json:"errors"`
			Origin  struct {
				Kind string `json:"kind"`
			} `json:"origin"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return fmt.Errorf("decode result event: %w", err)
		}
		if event.Origin.Kind != "task-notification" {
			description := ""
			if event.Subtype != "success" || event.IsError {
				description = strings.Join(event.Errors, "; ")
				if description == "" {
					description = "Claude result was not successful"
				}
			}
			a.finishTurn(description)
		}
	}
	return nil
}

func (a *traceAdapter) processAssistant(raw json.RawMessage) error {
	var event struct {
		Message struct {
			Content []struct {
				Type  string          `json:"type"`
				Text  string          `json:"text"`
				ID    string          `json:"id"`
				Name  string          `json:"name"`
				Input json.RawMessage `json:"input"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("decode assistant event: %w", err)
	}

	a.finishTurn("")
	a.turn = a.benchmark.StartTurn()
	if len(a.nextInput) != 0 {
		a.turn.SetLLMInput(strings.Join(a.nextInput, "\n"))
		a.nextInput = nil
	}

	var output []string
	for _, block := range event.Message.Content {
		switch block.Type {
		case "text":
			output = append(output, block.Text)
		case "tool_use":
			if block.ID == "" || block.Name == "" {
				return fmt.Errorf("tool_use block requires id and name")
			}
			tool := a.startTool(block.Name, block.Input)
			if previous := a.tools[block.ID]; previous != nil {
				previous.End()
			}
			a.tools[block.ID] = tool
		}
	}
	a.turn.SetLLMOutput(strings.Join(output, "\n"))
	return nil
}

func (a *traceAdapter) processUser(raw json.RawMessage) error {
	var event struct {
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("decode user event: %w", err)
	}
	content := bytes.TrimSpace(event.Message.Content)
	if len(content) == 0 || content[0] != '[' {
		return nil
	}
	var blocks []struct {
		Type      string          `json:"type"`
		ToolUseID string          `json:"tool_use_id"`
		Content   json.RawMessage `json:"content"`
		IsError   bool            `json:"is_error"`
	}
	if err := json.Unmarshal(content, &blocks); err != nil {
		return fmt.Errorf("decode user content blocks: %w", err)
	}
	for _, block := range blocks {
		if block.Type != "tool_result" {
			continue
		}
		if block.ToolUseID == "" {
			return fmt.Errorf("tool_result block requires tool_use_id")
		}
		output := traceText(block.Content)
		if tool := a.tools[block.ToolUseID]; tool != nil {
			tool.SetOutput(output)
			if block.IsError {
				tool.SetError(output)
			} else {
				tool.SetOK()
			}
			tool.End()
			delete(a.tools, block.ToolUseID)
		}
		a.nextInput = append(a.nextInput, output)
	}
	return nil
}

func (a *traceAdapter) startTool(name string, input json.RawMessage) tracing.ToolSpan {
	rawInput := strings.TrimSpace(string(input))
	if rawInput == "" {
		rawInput = "null"
	}
	generic := func() tracing.ToolSpan {
		span := a.turn.StartGenericToolSpan(name)
		span.SetInput(rawInput)
		return span
	}

	switch name {
	case "Bash":
		var value struct {
			Command string `json:"command"`
		}
		if json.Unmarshal(input, &value) != nil || value.Command == "" {
			return generic()
		}
		span := a.turn.StartBashToolSpan()
		span.SetInput(value.Command)
		return span
	case "Read":
		var value struct {
			FilePath string `json:"file_path"`
			Offset   int    `json:"offset"`
			Limit    int    `json:"limit"`
			Pages    string `json:"pages"`
		}
		if json.Unmarshal(input, &value) != nil || value.FilePath == "" {
			return generic()
		}
		span := a.turn.StartReadToolSpan()
		span.SetInput(value.FilePath, value.Offset, value.Limit)
		return span
	case "Edit":
		var value struct {
			FilePath   string `json:"file_path"`
			OldString  string `json:"old_string"`
			NewString  string `json:"new_string"`
			ReplaceAll bool   `json:"replace_all"`
		}
		if json.Unmarshal(input, &value) != nil || value.FilePath == "" {
			return generic()
		}
		span := a.turn.StartEditToolSpan()
		span.SetInput(value.FilePath, []tracing.Edit{{OldText: value.OldString, NewText: value.NewString}})
		return span
	case "Write":
		var value struct {
			FilePath string `json:"file_path"`
			Content  string `json:"content"`
		}
		if json.Unmarshal(input, &value) != nil || value.FilePath == "" {
			return generic()
		}
		span := a.turn.StartWriteToolSpan()
		span.SetInput(value.FilePath, value.Content)
		return span
	default:
		return generic()
	}
}

func traceText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	return strings.TrimSpace(string(raw))
}

func (a *traceAdapter) finishTurn(description string) {
	for id, tool := range a.tools {
		tool.SetError("Claude did not report a result for this tool call")
		tool.End()
		delete(a.tools, id)
	}
	if a.turn != nil {
		if description == "" {
			a.turn.SetOK()
		} else {
			a.turn.SetError(description)
		}
		a.turn.End()
		a.turn = nil
	}
}

func (a *traceAdapter) Close() {
	if a == nil || a.benchmark == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.finishTurn("Claude stream ended before a result")
	a.closed = true
}
