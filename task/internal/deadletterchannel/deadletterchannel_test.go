package deadletterchannel

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPromptIncludesRabbitMQQueuesAndConsumerRules(t *testing.T) {
	prompt, err := New([]int{19080, 19081}).Prompt()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"amqp://guest:guest@127.0.0.1:19081/", "messages", "dead-letters",
		`"amount"`, "negative", "manual acknowledgements", "requeue disabled", "x-death",
		"before and during its checks",
	} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestRabbitMQFeedbackValidationAndCleanup(t *testing.T) {
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
	stopApplication := startApplication(t, ports[0], handle.brokerURL())
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
		t.Fatalf("RabbitMQ container %q was not removed", containerName)
	}
}

func TestNewRejectsInvalidPorts(t *testing.T) {
	for _, ports := range [][]int{{19080}, {19080, 0}, {19080, 65536}} {
		if _, err := New(ports).Prompt(); err == nil {
			t.Errorf("New(%v) did not reject invalid ports", ports)
		}
	}
}

func requireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("Docker daemon is not available")
	}
}

func startApplication(t *testing.T, applicationPort int, brokerURL string) func() {
	t.Helper()
	connection, err := amqp.Dial(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	deliveries, err := channel.Consume(mainQueue, "test-application", false, false, false, false, nil)
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	go func() {
		for delivery := range deliveries {
			var message struct {
				Payload struct {
					Amount *float64 `json:"amount"`
				} `json:"payload"`
			}
			if json.Unmarshal(delivery.Body, &message) != nil || message.Payload.Amount == nil || *message.Payload.Amount < 0 {
				_ = delivery.Nack(false, false)
				continue
			}
			_ = delivery.Ack(false)
		}
	}()

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", applicationPort))
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/health" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}), ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = connection.Close()
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
