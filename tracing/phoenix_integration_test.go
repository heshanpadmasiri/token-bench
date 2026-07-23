package tracing_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"token-bench/tracing"
)

const defaultPhoenixTestImage = "arizephoenix/phoenix:19.4.0"

func TestPhoenixExportIntegration(t *testing.T) {
	collectorEndpoint := strings.TrimSpace(os.Getenv("PHOENIX_TEST_ENDPOINT"))
	apiEndpoint := strings.TrimSpace(os.Getenv("PHOENIX_TEST_API_ENDPOINT"))
	if collectorEndpoint == "" {
		if testing.Short() {
			t.Skip("set PHOENIX_TEST_ENDPOINT to test an existing Phoenix instance in short mode")
		}
		collectorEndpoint, apiEndpoint = startPhoenixContainer(t)
	} else if apiEndpoint == "" {
		apiEndpoint = phoenixAPIEndpoint(collectorEndpoint)
	}

	project := strings.TrimSpace(os.Getenv("PHOENIX_TEST_PROJECT"))
	if project == "" {
		project = fmt.Sprintf("token-bench-tracing-test-%d", time.Now().UnixNano())
	}
	headers := make(map[string]string)
	if key := strings.TrimSpace(os.Getenv("PHOENIX_TEST_API_KEY")); key != "" {
		headers["Authorization"] = "Bearer " + key
	}
	t.Logf("exporting test trace to Phoenix project %q at %s", project, apiEndpoint)

	tracer, err := tracing.NewPhoenix(tracing.PhoenixConfig{
		Endpoint:      collectorEndpoint,
		ProjectName:   project,
		Headers:       headers,
		ExportTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	benchmark := tracer.StartBenchmarkSpan("integration-test", "test-harness", "test-skill")

	firstTurn := benchmark.StartTurn()
	firstTurn.SetLLMInput("inspect the project and run its tests")
	search := firstTurn.StartGenericToolSpan("search")
	search.SetInput("find server entry points")
	search.SetOutput("main.go")
	search.End()
	bash := firstTurn.StartBashToolSpan()
	bash.SetInput("go test ./...")
	bash.SetOutput("ok")
	bash.End()
	firstTurn.SetLLMOutput("the tests pass; I will update the implementation")
	firstTurn.End()

	secondTurn := benchmark.StartTurn()
	secondTurn.SetLLMInput("apply the required implementation changes")
	edit := secondTurn.StartEditToolSpan()
	edit.SetInput("main.go", []tracing.Edit{{OldText: "old handler", NewText: "new handler"}})
	edit.SetOutput("updated main.go")
	edit.End()
	write := secondTurn.StartWriteToolSpan()
	write.SetInput("config.json", `{"enabled":true}`)
	write.SetOutput("wrote config.json")
	write.End()
	read := secondTurn.StartReadToolSpan()
	read.SetInput("main.go", 1, 80)
	read.SetOutput("package main")
	read.End()
	secondTurn.SetLLMOutput("implementation complete")
	secondTurn.End()

	benchmark.End()
	if err := tracer.Shutdown(); err != nil {
		t.Fatalf("flush trace to Phoenix: %v", err)
	}

	trace := waitForPhoenixTrace(t, apiEndpoint, project, headers)
	byName := make(map[string][]phoenixSpan, len(trace.Spans))
	for _, span := range trace.Spans {
		byName[span.Name] = append(byName[span.Name], span)
	}
	if len(trace.Spans) != 8 || len(byName["benchmark"]) != 1 || len(byName["llm"]) != 2 {
		t.Fatalf("Phoenix trace did not contain one benchmark and two LLM turns: %+v", trace.Spans)
	}
	root := byName["benchmark"][0]
	if root.SpanKind != "AGENT" || spanParentID(root) != "" {
		t.Fatalf("unexpected benchmark span: %+v", root)
	}
	llmIDs := make(map[string]struct{}, 2)
	for _, llm := range byName["llm"] {
		if llm.SpanKind != "LLM" || spanParentID(llm) != root.SpanID {
			t.Fatalf("unexpected LLM turn: %+v", llm)
		}
		llmIDs[llm.SpanID] = struct{}{}
	}
	tools := make(map[string]phoenixSpan, 5)
	for _, name := range []string{"search", "bash", "edit", "write", "read"} {
		if len(byName[name]) != 1 {
			t.Fatalf("expected exactly one %q tool span: %+v", name, trace.Spans)
		}
		tool := byName[name][0]
		if tool.SpanKind != "TOOL" {
			t.Fatalf("unexpected %q tool kind: %+v", name, tool)
		}
		if _, ok := llmIDs[spanParentID(tool)]; !ok {
			t.Fatalf("%q tool is not parented by an LLM turn: %+v", name, tool)
		}
		tools[name] = tool
	}
	firstParent := spanParentID(tools["search"])
	secondParent := spanParentID(tools["edit"])
	if spanParentID(tools["bash"]) != firstParent || spanParentID(tools["write"]) != secondParent || spanParentID(tools["read"]) != secondParent || firstParent == secondParent {
		t.Fatalf("tools were not grouped under the expected separate turns: %+v", tools)
	}
}

func startPhoenixContainer(t *testing.T) (string, string) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("Docker daemon is not available and PHOENIX_TEST_ENDPOINT is not set")
	}
	port := testPort(t)
	image := strings.TrimSpace(os.Getenv("PHOENIX_TEST_IMAGE"))
	if image == "" {
		image = defaultPhoenixTestImage
	}
	name := fmt.Sprintf("token-bench-phoenix-test-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	output, err := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm", "--name", name,
		"--publish", fmt.Sprintf("127.0.0.1:%d:6006", port), image).CombinedOutput()
	cancel()
	if err != nil {
		t.Fatalf("start Phoenix container: %v: %s", err, strings.TrimSpace(string(output)))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = exec.CommandContext(ctx, "docker", "rm", "--force", name).CombinedOutput()
	})

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		response, requestErr := client.Get(endpoint + "/healthz")
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return endpoint, endpoint
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
	t.Fatalf("Phoenix did not become healthy: %s", strings.TrimSpace(string(logs)))
	return "", ""
}

func testPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func phoenixAPIEndpoint(collectorEndpoint string) string {
	parsed, err := url.Parse(strings.TrimSpace(collectorEndpoint))
	if err != nil {
		return strings.TrimRight(collectorEndpoint, "/")
	}
	parsed.Path = strings.TrimSuffix(strings.TrimRight(parsed.Path, "/"), "/v1/traces")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

type phoenixTrace struct {
	TraceID string        `json:"trace_id"`
	Spans   []phoenixSpan `json:"spans"`
}

type phoenixSpan struct {
	SpanID   string  `json:"span_id"`
	ParentID *string `json:"parent_id"`
	Name     string  `json:"name"`
	SpanKind string  `json:"span_kind"`
}

func spanParentID(span phoenixSpan) string {
	if span.ParentID == nil {
		return ""
	}
	return *span.ParentID
}

func waitForPhoenixTrace(t *testing.T, apiEndpoint, project string, headers map[string]string) phoenixTrace {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	query := strings.TrimRight(apiEndpoint, "/") + "/v1/projects/" + url.PathEscape(project) + "/traces?include_spans=true"
	deadline := time.Now().Add(30 * time.Second)
	var lastStatus string
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, query, nil)
		if err != nil {
			t.Fatal(err)
		}
		for name, value := range headers {
			request.Header.Set(name, value)
		}
		response, err := client.Do(request)
		if err != nil {
			lastStatus = err.Error()
			time.Sleep(250 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			lastStatus = readErr.Error()
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if response.StatusCode == http.StatusOK {
			var result struct {
				Data []phoenixTrace `json:"data"`
			}
			if err := json.Unmarshal(body, &result); err != nil {
				lastStatus = fmt.Sprintf("decode response: %v", err)
			} else {
				for _, trace := range result.Data {
					if isCompleteIntegrationTrace(trace) {
						return trace
					}
				}
				lastStatus = fmt.Sprintf("project has %d traces but the complete integration trace is not available yet", len(result.Data))
			}
		} else {
			lastStatus = fmt.Sprintf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("trace did not appear in Phoenix project %q: %s", project, lastStatus)
	return phoenixTrace{}
}

func isCompleteIntegrationTrace(trace phoenixTrace) bool {
	if len(trace.Spans) != 8 {
		return false
	}
	counts := make(map[string]int, len(trace.Spans))
	for _, span := range trace.Spans {
		counts[span.Name]++
	}
	if counts["benchmark"] != 1 || counts["llm"] != 2 {
		return false
	}
	for _, name := range []string{"search", "bash", "edit", "write", "read"} {
		if counts[name] != 1 {
			return false
		}
	}
	return true
}
