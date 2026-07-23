package pi

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestObserveStatusFormatsStreamingTextAndTools(t *testing.T) {
	status := make(chan string, 1)
	agent := New(Config{Status: status})
	agent.observeStatus(map[string]json.RawMessage{
		"type":                  json.RawMessage(`"message_update"`),
		"assistantMessageEvent": json.RawMessage(`{"type":"text_delta","delta":"Building server"}`),
	})
	if actual := <-status; actual != "Pi\n\nBuilding server" {
		t.Fatalf("unexpected text status %q", actual)
	}
	agent.observeStatus(map[string]json.RawMessage{
		"type":     json.RawMessage(`"tool_execution_start"`),
		"toolName": json.RawMessage(`"bash"`),
		"args":     json.RawMessage(`{"command":"go test ./..."}`),
	})
	if actual := <-status; !strings.Contains(actual, "Using bash\ngo test ./...") {
		t.Fatalf("unexpected tool status %q", actual)
	}
}
