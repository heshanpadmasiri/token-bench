package claimcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPromptIncludesTwoServicesRedisAndBackends(t *testing.T) {
	prompt, err := New([]int{19080, 19081, 19082, 19083, 19084}).Prompt()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"Service A listens on port `19080`", "Service B listens on port `19081`",
		"Port: `19082`", "http://127.0.0.1:19083/claimed-messages",
		"http://127.0.0.1:19084/deliveries", "claim:<messageId>", "five-minute TTL",
		"only the original `userId`", "remove `messageId`", "HTTP 404",
		"Never stop, kill, replace, reconfigure, flush",
	} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestRedisFeedbackValidationAndCleanup(t *testing.T) {
	requireDocker(t)
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
	containerName := handle.containerName
	stopApplication := startApplication(t, ports[0], ports[1], fmt.Sprintf("127.0.0.1:%d", ports[2]), origin(ports[3])+"/claimed-messages", origin(ports[4])+"/deliveries")
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
	if err := exec.Command("docker", "inspect", containerName).Run(); err == nil {
		t.Fatalf("Redis container %q was not removed", containerName)
	}
}

func TestNewRejectsInvalidPorts(t *testing.T) {
	for _, ports := range [][]int{{19080}, {19080, 19081, 19082, 19083, 0}, {19080, 19081, 19082, 19083, 65536}} {
		if _, err := New(ports).Prompt(); err == nil {
			t.Errorf("New(%v) did not reject invalid ports", ports)
		}
	}
}

func startApplication(t *testing.T, serviceAPort, serviceBPort int, redisAddress, intermediaryURL, finalURL string) func() {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: redisAddress})
	var sequence atomic.Int64
	httpClient := &http.Client{Timeout: 5 * time.Second}

	serviceB := http.NewServeMux()
	serviceB.HandleFunc("GET /health", health)
	serviceB.HandleFunc("POST /messages", func(writer http.ResponseWriter, request *http.Request) {
		message, ok := decodeObject(writer, request)
		if !ok {
			return
		}
		if _, exists := message["userId"]; exists {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		messageID, ok := nonEmptyString(message["messageId"])
		if !ok {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		userID, err := client.Get(request.Context(), "claim:"+messageID).Result()
		if errors.Is(err, redis.Nil) {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		delete(message, "messageId")
		message["userId"] = userID
		status, responseBody, err := postJSON(request.Context(), httpClient, finalURL, message)
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		if status >= 200 && status < 300 {
			if client.Del(request.Context(), "claim:"+messageID).Err() != nil {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writer.WriteHeader(http.StatusAccepted)
			return
		}
		writer.WriteHeader(status)
		_, _ = writer.Write(responseBody)
	})
	serverB, listenerB := startTestServer(t, serviceBPort, serviceB)

	serviceA := http.NewServeMux()
	serviceA.HandleFunc("GET /health", health)
	serviceA.HandleFunc("POST /messages", func(writer http.ResponseWriter, request *http.Request) {
		message, ok := decodeObject(writer, request)
		if !ok {
			return
		}
		if _, exists := message["messageId"]; exists {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, exists := message["deliveryAddress"]; exists {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		userID, validID := nonEmptyString(message["userId"])
		_, validName := nonEmptyString(message["userName"])
		userAddress, validAddress := message["userAddress"].(map[string]any)
		if !validID || !validName || !validAddress || !completeAddress(userAddress) {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		messageID := fmt.Sprintf("message-%d-%d", time.Now().UnixNano(), sequence.Add(1))
		if err := client.Set(request.Context(), "claim:"+messageID, userID, claimTTL).Err(); err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		delete(message, "userId")
		message["messageId"] = messageID
		status, responseBody, err := postJSON(request.Context(), httpClient, intermediaryURL, message)
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		if status >= 200 && status < 300 {
			writer.WriteHeader(http.StatusAccepted)
			return
		}
		writer.WriteHeader(status)
		_, _ = writer.Write(responseBody)
	})
	serverA, listenerA := startTestServer(t, serviceAPort, serviceA)

	var once sync.Once
	return func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = serverA.Shutdown(ctx)
			_ = serverB.Shutdown(ctx)
			_ = listenerA.Close()
			_ = listenerB.Close()
			_ = client.Close()
		})
	}
}

func health(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) }

func decodeObject(writer http.ResponseWriter, request *http.Request) (map[string]any, bool) {
	var message map[string]any
	decoder := json.NewDecoder(request.Body)
	if decoder.Decode(&message) != nil || message == nil {
		writer.WriteHeader(http.StatusBadRequest)
		return nil, false
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		writer.WriteHeader(http.StatusBadRequest)
		return nil, false
	}
	return message, true
}

func nonEmptyString(value any) (string, bool) {
	text, ok := value.(string)
	return text, ok && text != ""
}

func completeAddress(value map[string]any) bool {
	for _, field := range []string{"street", "city", "state", "postalCode", "country"} {
		if _, ok := nonEmptyString(value[field]); !ok {
			return false
		}
	}
	return true
}

func postJSON(ctx context.Context, client *http.Client, endpoint string, value map[string]any) (int, []byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	return response.StatusCode, responseBody, err
}

func startTestServer(t *testing.T, port int, handler http.Handler) (*http.Server, net.Listener) {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	return server, listener
}

func requireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("Docker daemon is not available")
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
