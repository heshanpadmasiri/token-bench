package claude

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"token-bench/tracing"
)

func TestTraceAdapterMapsAssistantEventsAndToolResults(t *testing.T) {
	benchmark := &recordingBenchmark{}
	adapter := newTraceAdapter(benchmark, model)
	adapter.SetNextInput("build the server")
	processTraceEvent(t, adapter, "assistant", `{
		"type":"assistant","message":{"content":[
			{"type":"text","text":"checking"},
			{"type":"tool_use","id":"bash-1","name":"Bash","input":{"command":"go test ./..."}},
			{"type":"tool_use","id":"custom-1","name":"Search","input":{"query":"handler"}}
		]}}`)
	processTraceEvent(t, adapter, "user", `{
		"type":"user","message":{"content":[
			{"type":"tool_result","tool_use_id":"bash-1","content":"ok"},
			{"type":"tool_result","tool_use_id":"custom-1","content":"main.go"}
		]}}`)
	processTraceEvent(t, adapter, "assistant", `{"type":"assistant","message":{"content":[{"type":"text","text":"done"}]}}`)
	processTraceEvent(t, adapter, "result", `{"type":"result","subtype":"success"}`)

	if len(benchmark.turns) != 2 {
		t.Fatalf("got %d turns, want 2", len(benchmark.turns))
	}
	first, second := benchmark.turns[0], benchmark.turns[1]
	if first.input != "build the server" || first.output != "checking" || first.status != "OK" || !first.ended {
		t.Fatalf("unexpected first turn: %+v", first)
	}
	if second.input != "ok\nmain.go" || second.output != "done" || second.status != "OK" || !second.ended {
		t.Fatalf("unexpected second turn: %+v", second)
	}
	if len(first.tools) != 2 {
		t.Fatalf("got %d tools, want 2", len(first.tools))
	}
	assertRecordedTool(t, first.tools[0], "bash", `{"command":"go test ./..."}`, "ok")
	assertRecordedTool(t, first.tools[1], "Search", `{"query":"handler"}`, "main.go")
}

func TestTraceAdapterMarksFailedToolsAndResults(t *testing.T) {
	benchmark := &recordingBenchmark{}
	adapter := newTraceAdapter(benchmark, model)
	processTraceEvent(t, adapter, "assistant", `{"type":"assistant","message":{"content":[{"type":"tool_use","id":"bash","name":"Bash","input":{"command":"false"}}]}}`)
	processTraceEvent(t, adapter, "user", `{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"bash","content":"exit 1","is_error":true}]}}`)
	processTraceEvent(t, adapter, "result", `{"type":"result","subtype":"error","is_error":true,"errors":["model failed"]}`)

	turn := benchmark.turns[0]
	if turn.status != "ERROR: model failed" {
		t.Fatalf("unexpected turn status: %q", turn.status)
	}
	if status := turn.tools[0].status; status != "ERROR: exit 1" {
		t.Fatalf("unexpected tool status: %q", status)
	}
}

func TestTraceAdapterRecordsConcreteAndFallbackModelUsage(t *testing.T) {
	benchmark := &recordingBenchmark{}
	adapter := newTraceAdapter(benchmark, "configured-model")
	processTraceEvent(t, adapter, "result", `{
		"type":"result","subtype":"success","modelUsage":{
			"model-b":{"inputTokens":2,"outputTokens":3,"cacheReadInputTokens":5,"cacheCreationInputTokens":7},
			"model-a":{"inputTokens":11,"outputTokens":13,"cacheReadInputTokens":17,"cacheCreationInputTokens":19}
		}}`)
	processTraceEvent(t, adapter, "result", `{
		"type":"result","subtype":"success",
		"usage":{"input_tokens":23,"output_tokens":29,"cache_read_input_tokens":31,"cache_creation_input_tokens":37}
	}`)

	want := []tracing.ModelUsage{
		{Model: "model-a", Input: 11, Output: 13, CacheRead: 17, CacheWrite: 19},
		{Model: "model-b", Input: 2, Output: 3, CacheRead: 5, CacheWrite: 7},
		{Model: "configured-model", Input: 23, Output: 29, CacheRead: 31, CacheWrite: 37},
	}
	if !reflect.DeepEqual(benchmark.usage, want) {
		t.Fatalf("unexpected traced usage:\nwant: %+v\n got: %+v", want, benchmark.usage)
	}
}

func TestTraceAdapterMapsKnownClaudeTools(t *testing.T) {
	benchmark := &recordingBenchmark{}
	adapter := newTraceAdapter(benchmark, model)
	processTraceEvent(t, adapter, "assistant", `{
		"type":"assistant","message":{"content":[
			{"type":"tool_use","id":"read","name":"Read","input":{"file_path":"main.go","offset":2,"limit":10}},
			{"type":"tool_use","id":"edit","name":"Edit","input":{"file_path":"main.go","old_string":"old","new_string":"new"}},
			{"type":"tool_use","id":"write","name":"Write","input":{"file_path":"go.mod","content":"module example"}},
			{"type":"tool_use","id":"pages","name":"Read","input":{"file_path":"doc.pdf","pages":"1-2"}}
		]}}`)
	processTraceEvent(t, adapter, "user", `{"type":"user","message":{"content":[
		{"type":"tool_result","tool_use_id":"read","content":""},
		{"type":"tool_result","tool_use_id":"edit","content":""},
		{"type":"tool_result","tool_use_id":"write","content":""},
		{"type":"tool_result","tool_use_id":"pages","content":""}
	]}}`)
	adapter.Close()

	tools := benchmark.turns[0].tools
	assertRecordedTool(t, tools[0], "read", `{"path":"main.go","offset":2,"limit":10}`, "")
	assertRecordedTool(t, tools[1], "edit", `{"path":"main.go","edits":[{"old_text":"old","new_text":"new"}]}`, "")
	assertRecordedTool(t, tools[2], "write", `{"path":"go.mod","content":"module example"}`, "")
	assertRecordedTool(t, tools[3], "read", `{"path":"doc.pdf","offset":0,"limit":0}`, "")
	for _, tool := range tools {
		if !tool.ended {
			t.Errorf("tool %q was not ended", tool.name)
		}
	}
}

func processTraceEvent(t *testing.T, adapter *traceAdapter, eventType, event string) {
	t.Helper()
	if err := adapter.Process(eventType, json.RawMessage(event)); err != nil {
		t.Fatal(err)
	}
}

func assertRecordedTool(t *testing.T, tool *recordingTool, name, input, output string) {
	t.Helper()
	if tool.name != name || tool.input != input || tool.output != output || tool.status != "OK" || !tool.ended {
		t.Errorf("unexpected tool: got %+v, want name=%q input=%q output=%q ended", tool, name, input, output)
	}
}

type recordingBenchmark struct {
	turns []*recordingTurn
	usage []tracing.ModelUsage
}

func (b *recordingBenchmark) StartTurn() tracing.TurnSpan {
	turn := &recordingTurn{}
	b.turns = append(b.turns, turn)
	return turn
}
func (b *recordingBenchmark) AddModelUsage(usage []tracing.ModelUsage) {
	b.usage = append(b.usage, usage...)
}
func (*recordingBenchmark) SetOK()          {}
func (*recordingBenchmark) SetError(string) {}
func (*recordingBenchmark) End()            {}

type recordingTurn struct {
	input  string
	output string
	ended  bool
	status string
	tools  []*recordingTool
}

func (t *recordingTurn) SetOK()                      { t.status = "OK" }
func (t *recordingTurn) SetError(description string) { t.status = "ERROR: " + description }
func (t *recordingTurn) End()                        { t.ended = true }
func (t *recordingTurn) SetLLMInput(value string)    { t.input = value }
func (t *recordingTurn) SetLLMOutput(value string)   { t.output = value }
func (t *recordingTurn) addTool(name string) *recordingTool {
	tool := &recordingTool{name: name}
	t.tools = append(t.tools, tool)
	return tool
}
func (t *recordingTurn) StartGenericToolSpan(name string) tracing.GenericToolSpan {
	return &recordingGenericTool{recordingTool: t.addTool(name)}
}
func (t *recordingTurn) StartBashToolSpan() tracing.BashToolSpan {
	return &recordingBashTool{recordingTool: t.addTool("bash")}
}
func (t *recordingTurn) StartEditToolSpan() tracing.EditToolSpan {
	return &recordingEditTool{recordingTool: t.addTool("edit")}
}
func (t *recordingTurn) StartWriteToolSpan() tracing.WriteToolSpan {
	return &recordingWriteTool{recordingTool: t.addTool("write")}
}
func (t *recordingTurn) StartReadToolSpan() tracing.ReadToolSpan {
	return &recordingReadTool{recordingTool: t.addTool("read")}
}

type recordingTool struct {
	name   string
	input  string
	output string
	ended  bool
	status string
}

func (t *recordingTool) SetOK()                 { t.status = "OK" }
func (t *recordingTool) SetError(value string)  { t.status = "ERROR: " + value }
func (t *recordingTool) End()                   { t.ended = true }
func (t *recordingTool) SetOutput(value string) { t.output = value }

type recordingGenericTool struct{ *recordingTool }

func (t *recordingGenericTool) SetInput(value string) { t.input = value }

type recordingBashTool struct{ *recordingTool }

func (t *recordingBashTool) SetInput(command string) {
	t.input = fmt.Sprintf(`{"command":%q}`, command)
}

type recordingEditTool struct{ *recordingTool }

func (t *recordingEditTool) SetInput(path string, edits []tracing.Edit) {
	type serializedEdit struct {
		OldText string `json:"old_text"`
		NewText string `json:"new_text"`
	}
	serialized := make([]serializedEdit, len(edits))
	for index, edit := range edits {
		serialized[index] = serializedEdit{OldText: edit.OldText, NewText: edit.NewText}
	}
	value := struct {
		Path  string           `json:"path"`
		Edits []serializedEdit `json:"edits"`
	}{Path: path, Edits: serialized}
	encoded, _ := json.Marshal(value)
	t.input = string(encoded)
}

type recordingWriteTool struct{ *recordingTool }

func (t *recordingWriteTool) SetInput(path, content string) {
	encoded, _ := json.Marshal(struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}{path, content})
	t.input = string(encoded)
}

type recordingReadTool struct{ *recordingTool }

func (t *recordingReadTool) SetInput(path string, offset, limit int) {
	encoded, _ := json.Marshal(struct {
		Path   string `json:"path"`
		Offset int    `json:"offset"`
		Limit  int    `json:"limit"`
	}{path, offset, limit})
	t.input = string(encoded)
}
