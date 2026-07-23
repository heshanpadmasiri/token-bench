package multiconsumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestPromptIncludesInfrastructureSchemaAndSamples(t *testing.T) {
	prompt, err := New([]int{19080, 19081, 19082}).Prompt()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"127.0.0.1:19081", "127.0.0.1:19082/shipment_ops", kafkaUser, kafkaPassword,
		shipmentTopic, pickupTopic, digitalTopic, "CREATE TABLE fulfillment_events",
		`"fulfillmentType":"store_pickup"`, "never stop, kill, reconfigure, or replace",
	} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestNewRejectsInvalidPorts(t *testing.T) {
	for _, ports := range [][]int{{19080}, {19080, 19081, 0}, {19080, 19081, 65536}} {
		handle := New(ports)
		if _, err := handle.Prompt(); err == nil {
			t.Errorf("New(%v) did not reject invalid ports", ports)
		}
	}
}

func TestFeedbackAndValidation(t *testing.T) {
	requireDocker(t)
	ports := testPorts(t, requiredPorts)
	handle := New(ports)
	cleanup, err := handle.Setup()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })
	stop := startReferenceApplication(t, handle)
	t.Cleanup(stop)

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

func startReferenceApplication(t *testing.T, handle *multiConsumer) func() {
	t.Helper()
	ctx, cancelConsumers := context.WithCancel(context.Background())
	client, err := kgo.NewClient(
		kgo.SeedBrokers(handle.kafkaBroker()),
		kgo.SASL(plain.Auth{User: kafkaUser, Pass: kafkaPassword}.AsMechanism()),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(shipmentTopic, pickupTopic, digitalTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	connections := make(map[fulfillmentType]*pgx.Conn)
	for _, route := range routes {
		connection, err := pgx.Connect(ctx, handle.databaseURL(route.database))
		if err != nil {
			t.Fatal(err)
		}
		connections[route.kind] = connection
	}
	go func() {
		for ctx.Err() == nil {
			fetches := client.PollFetches(ctx)
			fetches.EachRecord(func(record *kgo.Record) {
				var event map[string]any
				decoder := json.NewDecoder(bytes.NewReader(record.Value))
				decoder.UseNumber()
				if decoder.Decode(&event) != nil {
					return
				}
				route := routeForTopic(record.Topic)
				occurredAt, _ := time.Parse(time.RFC3339, event["occurredAt"].(string))
				_, err := connections[route].Exec(ctx, `INSERT INTO fulfillment_events (event_id, order_id, occurred_at, customer_id, total_amount, currency, payload)
VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (event_id) DO NOTHING`,
					event["eventId"], event["orderId"], occurredAt, event["customerId"], event["totalAmount"].(json.Number).String(), event["currency"], record.Value)
				if err == nil {
					_ = client.CommitRecords(ctx, record)
				}
			})
		}
	}()

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", handle.applicationPort))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: time.Second}
	server.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/health" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		if request.Method != http.MethodPost || request.URL.Path != "/orders" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(request.Body)
		event, kind, valid := validReferenceEvent(body)
		if err != nil || !valid {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		record := &kgo.Record{Topic: routeFor(kind).topic, Key: []byte(event["eventId"].(string)), Value: body}
		if err := client.ProduceSync(request.Context(), record).FirstErr(); err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(writer, `{"status":"accepted","eventId":"%s"}`, event["eventId"])
	})
	go func() { _ = server.Serve(listener) }()
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		cancelConsumers()
		client.Close()
		for _, connection := range connections {
			_ = connection.Close(shutdownCtx)
		}
	}
}

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

func validReferenceEvent(body []byte) (map[string]any, fulfillmentType, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var event map[string]any
	if decoder.Decode(&event) != nil {
		return nil, "", false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, "", false
	}
	for _, field := range []string{"eventId", "orderId", "customerId"} {
		value, ok := event[field].(string)
		if !ok || value == "" {
			return nil, "", false
		}
	}
	occurredAt, ok := event["occurredAt"].(string)
	if !ok {
		return nil, "", false
	}
	if _, err := time.Parse(time.RFC3339, occurredAt); err != nil {
		return nil, "", false
	}
	kindValue, ok := event["fulfillmentType"].(string)
	kind := fulfillmentType(kindValue)
	if !ok || routeFor(kind).topic == "" {
		return nil, "", false
	}
	amount, ok := event["totalAmount"].(json.Number)
	if !ok {
		return nil, "", false
	}
	numeric, err := amount.Float64()
	if err != nil || numeric < 0 {
		return nil, "", false
	}
	currency, ok := event["currency"].(string)
	if !ok || !currencyPattern.MatchString(currency) {
		return nil, "", false
	}
	return event, kind, true
}

func routeForTopic(topic string) fulfillmentType {
	for _, route := range routes {
		if route.topic == topic {
			return route.kind
		}
	}
	return ""
}

func requireDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Docker integration test in short mode")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("Docker is unavailable")
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
