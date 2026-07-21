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
}

func TestPromptIncludesBackendsAndExamples(t *testing.T) {
	ports := []int{19080, 19081, 19082, 19083}
	prompt, err := New(ports).Prompt()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"127.0.0.1:19081", "127.0.0.1:19082", "127.0.0.1:19083", `"product": "widget"`, `"quotes"`} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestNewRejectsWrongPortCount(t *testing.T) {
	if _, err := New([]int{19080}).Prompt(); err == nil {
		t.Fatal("expected wrong port count to fail")
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
		if err != nil || !json.Valid(body) {
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
