package claude

import (
	"encoding/json"
	"fmt"
	"strings"

	"token-bench/harness/internal/shared"
)

func publishStatus(status chan string, eventType string, raw json.RawMessage) {
	message := claudeStatus(eventType, raw)
	if message != "" {
		shared.PublishStatus(status, message)
	}
}

func claudeStatus(eventType string, raw json.RawMessage) string {
	switch eventType {
	case "assistant":
		var event struct {
			Message struct {
				Content []struct {
					Type  string          `json:"type"`
					Text  string          `json:"text"`
					Name  string          `json:"name"`
					Input json.RawMessage `json:"input"`
				} `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal(raw, &event) != nil {
			return ""
		}
		var text []string
		var tool string
		for _, block := range event.Message.Content {
			switch block.Type {
			case "text":
				if value := strings.TrimSpace(block.Text); value != "" {
					text = append(text, value)
				}
			case "tool_use":
				tool = formatClaudeTool(block.Name, block.Input)
			}
		}
		if tool != "" {
			return "Claude\n\n" + tool
		}
		if len(text) != 0 {
			return "Claude\n\n" + strings.Join(text, "\n\n")
		}
	case "result":
		var event resultEvent
		if json.Unmarshal(raw, &event) != nil || event.Origin.Kind == "task-notification" {
			return ""
		}
		if event.Subtype == "success" && !event.IsError {
			return "Claude\n\nCompleted the current task."
		}
		detail := strings.Join(event.Errors, "\n")
		if detail == "" {
			detail = "The current task failed."
		}
		return "Claude\n\n" + detail
	}
	return ""
}

func formatClaudeTool(name string, input json.RawMessage) string {
	var arguments map[string]any
	_ = json.Unmarshal(input, &arguments)
	for _, key := range []string{"command", "file_path", "pattern", "query"} {
		if value, ok := arguments[key].(string); ok && value != "" {
			return fmt.Sprintf("Using %s\n%s", name, value)
		}
	}
	value := strings.TrimSpace(string(input))
	if value == "" || value == "null" || value == "{}" {
		return "Using " + name
	}
	return fmt.Sprintf("Using %s\n%s", name, value)
}
