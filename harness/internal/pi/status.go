package pi

import (
	"encoding/json"
	"fmt"
	"strings"

	"token-bench/harness/internal/shared"
)

func (p *pi) observeStatus(event map[string]json.RawMessage) {
	switch stringValue(event["type"]) {
	case "message_update":
		p.statusMu.Lock()
		defer p.statusMu.Unlock()
		var update struct {
			Type    string `json:"type"`
			Delta   string `json:"delta"`
			Content string `json:"content"`
		}
		if json.Unmarshal(event["assistantMessageEvent"], &update) != nil {
			return
		}
		switch update.Type {
		case "text_start":
			p.statusText.Reset()
		case "text_delta":
			p.statusText.WriteString(update.Delta)
			p.publishStatus(strings.TrimSpace(p.statusText.String()))
		case "text_end":
			if update.Content != "" {
				p.publishStatus(strings.TrimSpace(update.Content))
			}
		}
	case "tool_execution_start":
		var tool struct {
			Name string         `json:"toolName"`
			Args map[string]any `json:"args"`
		}
		raw, _ := json.Marshal(event)
		if json.Unmarshal(raw, &tool) == nil {
			p.publishStatus(formatPiTool(tool.Name, tool.Args))
		}
	case "tool_execution_end":
		var tool struct {
			Name    string `json:"toolName"`
			IsError bool   `json:"isError"`
		}
		raw, _ := json.Marshal(event)
		if json.Unmarshal(raw, &tool) == nil && tool.IsError {
			p.publishStatus(fmt.Sprintf("Tool %s failed.", tool.Name))
		}
	case "compaction_start":
		p.publishStatus("Compacting the conversation context…")
	case "auto_retry_start":
		p.publishStatus("Waiting to retry the model request…")
	case "agent_settled":
		p.publishStatus("Completed the current task.")
	}
}

func (p *pi) publishStatus(message string) {
	if message != "" {
		shared.PublishStatus(p.config.Status, "Pi\n\n"+message)
	}
}

func formatPiTool(name string, arguments map[string]any) string {
	for _, key := range []string{"command", "path", "file_path", "pattern", "query"} {
		if value, ok := arguments[key].(string); ok && value != "" {
			return fmt.Sprintf("Using %s\n%s", name, value)
		}
	}
	if len(arguments) == 0 {
		return "Using " + name
	}
	encoded, _ := json.Marshal(arguments)
	return fmt.Sprintf("Using %s\n%s", name, encoded)
}
