package claude

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestClaudeStatusFormatsAssistantAndToolActivity(t *testing.T) {
	assistant := json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"Inspecting the project"}]}}`)
	if actual := claudeStatus("assistant", assistant); actual != "Claude\n\nInspecting the project" {
		t.Fatalf("unexpected assistant status %q", actual)
	}
	tool := json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"go test ./..."}}]}}`)
	actual := claudeStatus("assistant", tool)
	if !strings.Contains(actual, "Using Bash\ngo test ./...") {
		t.Fatalf("unexpected tool status %q", actual)
	}
}
