package contentbasedrouting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
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
}

func startRouter(t *testing.T, ports []int) func() {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[0]))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: time.Second}
	server.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
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
		response, err := http.DefaultClient.Do(outgoing)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
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
