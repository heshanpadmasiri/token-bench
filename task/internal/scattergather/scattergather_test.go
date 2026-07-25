package scattergather

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFeedbackAndValidation(t *testing.T) {
	ports := testPorts(t, requiredPorts)
	handle := New(ports)
	cleanup, err := handle.Setup()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })
	if _, err := handle.Setup(); err == nil {
		t.Fatal("second Setup unexpectedly succeeded")
	}
	stopApplication := startApplication(t, ports)
	t.Cleanup(stopApplication)

	feedback, err := handle.Feedback()
	if err != nil {
		t.Fatal(err)
	}
	if feedback != "" {
		t.Fatal(feedback)
	}
	passed, err := handle.Validation()
	if err != nil {
		t.Fatal(err)
	}
	if !passed {
		t.Fatal("hidden validation failed")
	}
	stopApplication()
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
}

func TestBackendsReturnSampleQuotesBeforeChecks(t *testing.T) {
	ports := testPorts(t, requiredPorts)
	handle := New(ports)
	cleanup, err := handle.Setup()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })

	for index, supplier := range []string{"alpha", "beta", "gamma"} {
		response, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/quotes", ports[index+1]), "application/json", strings.NewReader(`{"product":"widget","quantity":2}`))
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s backend returned HTTP %d: %s", supplier, response.StatusCode, body)
		}
		var quote struct {
			Supplier string `json:"supplier"`
		}
		if err := json.Unmarshal(body, &quote); err != nil {
			t.Fatalf("%s backend returned invalid JSON: %v", supplier, err)
		}
		if quote.Supplier != supplier {
			t.Errorf("expected supplier %q, got %q", supplier, quote.Supplier)
		}
	}
}

func TestPromptIncludesBackendsAndExamples(t *testing.T) {
	ports := []int{19080, 19081, 19082, 19083}
	prompt, err := New(ports).Prompt()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"127.0.0.1:19081", "127.0.0.1:19082", "127.0.0.1:19083",
		`"$schema"`, `"product": "widget"`, `"quotes"`,
		"already running and are owned by the benchmark",
		"Do not start replacement backends",
		"do not stop, kill, or otherwise modify any process listening on the backend ports",
	} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestNewRejectsInvalidPorts(t *testing.T) {
	for _, ports := range [][]int{{19080}, {19080, 19081, 19082, 0}, {19080, 19081, 19082, 65536}} {
		if _, err := New(ports).Prompt(); err == nil {
			t.Errorf("New(%v) did not reject invalid ports", ports)
		}
	}
}

func startApplication(t *testing.T, ports []int) func() {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[0]))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(writer, "ok")
	})
	mux.HandleFunc("POST /quotes", func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		var input map[string]any
		if err != nil || json.Unmarshal(body, &input) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, ok := input["product"].(string); !ok {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, ok := input["quantity"].(float64); !ok {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		type result struct {
			quote json.RawMessage
			err   error
		}
		results := make(chan result, 3)
		var group sync.WaitGroup
		for _, port := range ports[1:] {
			group.Add(1)
			go func(port int) {
				defer group.Done()
				endpoint := fmt.Sprintf("http://127.0.0.1:%d%s", port, request.URL.RequestURI())
				outgoing, err := http.NewRequestWithContext(request.Context(), request.Method, endpoint, bytes.NewReader(body))
				if err != nil {
					results <- result{err: err}
					return
				}
				outgoing.Header = request.Header.Clone()
				removeHopHeaders(outgoing.Header)
				response, err := http.DefaultClient.Do(outgoing)
				if err != nil {
					results <- result{err: err}
					return
				}
				defer response.Body.Close()
				quote, err := io.ReadAll(response.Body)
				if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
					err = fmt.Errorf("backend returned %d", response.StatusCode)
				}
				if err == nil && !json.Valid(quote) {
					err = fmt.Errorf("backend returned invalid JSON")
				}
				results <- result{quote: quote, err: err}
			}(port)
		}
		group.Wait()
		close(results)
		quotes := make([]json.RawMessage, 0, 3)
		for current := range results {
			if current.err != nil {
				writer.WriteHeader(http.StatusBadGateway)
				return
			}
			quotes = append(quotes, current.quote)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(struct {
			Quotes []json.RawMessage `json:"quotes"`
		}{quotes})
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
}

func removeHopHeaders(header http.Header) {
	for _, connection := range header.Values("Connection") {
		for name := range strings.SplitSeq(connection, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(name)
	}
}

func testPorts(t *testing.T, count int) []int {
	t.Helper()
	ports := make([]int, 0, count)
	seen := make(map[int]struct{}, count)
	for len(ports) < count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		if _, duplicate := seen[port]; duplicate {
			continue
		}
		seen[port] = struct{}{}
		ports = append(ports, port)
	}
	return ports
}
