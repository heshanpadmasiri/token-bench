package contentbasedrouting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPromptIncludesBackendsSchemaAndSamples(t *testing.T) {
	prompt, err := New([]int{19080, 19081, 19082}).Prompt()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"127.0.0.1:19081", "127.0.0.1:19082", `"$schema"`, `"messageType": "order"`, `"backend": "invoices"`, "Never stop, kill"} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestNewRejectsInvalidPorts(t *testing.T) {
	for _, ports := range [][]int{{19080}, {19080, 19081, 0}, {19080, 19081, 65536}} {
		if _, err := New(ports).Prompt(); err == nil {
			t.Errorf("New(%v) did not reject invalid ports", ports)
		}
	}
}

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
	stopRouter := startRouter(t, ports)
	t.Cleanup(stopRouter)

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
	stopRouter()
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
}

func startRouter(t *testing.T, ports []int) func() {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[0]))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: time.Second}
	server.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/health" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		var message map[string]any
		if json.Unmarshal(body, &message) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		messageType, ok := message["messageType"].(string)
		if !ok {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		port := 0
		switch messageType {
		case "order":
			port = ports[1]
		case "invoice":
			port = ports[2]
		default:
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		endpoint := fmt.Sprintf("http://127.0.0.1:%d%s", port, request.URL.RequestURI())
		outgoing, err := http.NewRequest(request.Method, endpoint, bytes.NewReader(body))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		outgoing.Header = request.Header.Clone()
		removeHopHeaders(outgoing.Header)
		response, err := http.DefaultClient.Do(outgoing)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		removeHopHeaders(response.Header)
		for name, values := range response.Header {
			for _, value := range values {
				writer.Header().Add(name, value)
			}
		}
		writer.WriteHeader(response.StatusCode)
		_, _ = io.Copy(writer, response.Body)
	})
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
