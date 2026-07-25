// Package multiconsumer implements the Multi-Consumer task with benchmark-owned
// Dockerized Kafka and PostgreSQL services.
package multiconsumer

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"token-bench/task/internal/jsoncompare"
)

const (
	requiredPorts = 3
	kafkaImage    = "apache/kafka:3.9.1"
	postgresImage = "postgres:16-alpine"
	databaseUser  = "tokenbench"
	databasePass  = "tokenbench"
	adminDatabase = "tokenbench"
	kafkaUser     = "tokenbench"
	kafkaPassword = "tokenbench-secret"
	consumerGroup = "token-bench-multi-consumer"
	shipmentTopic = "orders.fulfillment.shipment.v1"
	pickupTopic   = "orders.fulfillment.pickup.v1"
	digitalTopic  = "orders.fulfillment.digital.v1"
	shipmentDB    = "shipment_ops"
	pickupDB      = "pickup_ops"
	digitalDB     = "digital_ops"
	checkTimeout  = 30 * time.Second
)

//go:embed prompt.md.tmpl
var promptTemplate string

type fulfillmentType string

const (
	shipment    fulfillmentType = "shipment"
	storePickup fulfillmentType = "store_pickup"
	digital     fulfillmentType = "digital"
)

type routeSpec struct {
	kind     fulfillmentType
	topic    string
	database string
}

var routes = []routeSpec{
	{kind: shipment, topic: shipmentTopic, database: shipmentDB},
	{kind: storePickup, topic: pickupTopic, database: pickupDB},
	{kind: digital, topic: digitalTopic, database: digitalDB},
}

type multiConsumer struct {
	applicationPort   int
	kafkaPort         int
	postgresPort      int
	kafkaContainer    string
	postgresContainer string
	kafkaClient       *kgo.Client
	admin             *kadm.Client
	databases         map[fulfillmentType]*pgx.Conn
	initErr           error
}

type storedEvent struct {
	eventID     string
	orderID     string
	occurredAt  time.Time
	customerID  string
	totalAmount string
	currency    string
	payload     []byte
}

type check struct {
	name           string
	body           string
	eventID        string
	expectedRoute  fulfillmentType
	expectedStatus int
	repeat         int
	blockDatabase  bool
}

type checkResult struct {
	passed   bool
	feedback string
}

// PromptTemplate returns the unrendered task requirements.
func PromptTemplate() string { return promptTemplate }

// New initializes the private Multi-Consumer implementation.
func New(ports []int) *multiConsumer {
	handle := &multiConsumer{databases: make(map[fulfillmentType]*pgx.Conn)}
	if len(ports) != requiredPorts {
		handle.initErr = fmt.Errorf("multi consumer requires %d ports, got %d", requiredPorts, len(ports))
		return handle
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			handle.initErr = fmt.Errorf("invalid allocated port %d", port)
			return handle
		}
	}
	handle.applicationPort, handle.kafkaPort, handle.postgresPort = ports[0], ports[1], ports[2]
	return handle
}

func (m *multiConsumer) Setup() (func() error, error) {
	if m.initErr != nil {
		return nil, m.initErr
	}
	if m.kafkaContainer != "" || m.postgresContainer != "" {
		return nil, errors.New("multi consumer is already set up")
	}
	suffix := fmt.Sprintf("%d-%d", m.applicationPort, time.Now().UnixNano())
	m.kafkaContainer = "token-bench-kafka-" + suffix
	m.postgresContainer = "token-bench-postgres-multi-" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := m.startKafka(ctx); err != nil {
		_ = m.cleanup()
		return nil, err
	}
	if err := m.startPostgres(ctx); err != nil {
		_ = m.cleanup()
		return nil, err
	}
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() { cleanupErr = m.cleanup() })
		return cleanupErr
	}, nil
}

func (m *multiConsumer) startKafka(ctx context.Context) error {
	jaas := "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"broker\" password=\"broker-secret\" user_broker=\"broker-secret\" user_tokenbench=\"" + kafkaPassword + "\";"
	arguments := []string{"run", "--detach", "--rm", "--name", m.kafkaContainer,
		"--publish", fmt.Sprintf("127.0.0.1:%d:9092", m.kafkaPort),
		"--env", "KAFKA_NODE_ID=1",
		"--env", "KAFKA_PROCESS_ROLES=broker,controller",
		"--env", "KAFKA_LISTENERS=EXTERNAL://:9092,CONTROLLER://:9093",
		"--env", fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=EXTERNAL://127.0.0.1:%d", m.kafkaPort),
		"--env", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT",
		"--env", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"--env", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"--env", "KAFKA_INTER_BROKER_LISTENER_NAME=EXTERNAL",
		"--env", "KAFKA_SASL_ENABLED_MECHANISMS=PLAIN",
		"--env", "KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN",
		"--env", "KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG=" + jaas,
		"--env", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"--env", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"--env", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"--env", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"--env", "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
		kafkaImage}
	output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("start Kafka container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	m.kafkaClient, err = kgo.NewClient(
		kgo.SeedBrokers(m.kafkaBroker()),
		kgo.SASL(plain.Auth{User: kafkaUser, Pass: kafkaPassword}.AsMechanism()),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	for {
		err = m.kafkaClient.Ping(ctx)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Kafka: %w", errors.Join(err, ctx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
	m.admin = kadm.NewClient(m.kafkaClient)
	responses, err := m.admin.CreateTopics(ctx, 1, 1, nil, shipmentTopic, pickupTopic, digitalTopic)
	if err != nil {
		return fmt.Errorf("create Kafka topics: %w", err)
	}
	for topic, response := range responses {
		if response.Err != nil {
			return fmt.Errorf("create Kafka topic %s: %w", topic, response.Err)
		}
	}
	return nil
}

func (m *multiConsumer) startPostgres(ctx context.Context) error {
	output, err := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm", "--name", m.postgresContainer,
		"--env", "POSTGRES_USER="+databaseUser,
		"--env", "POSTGRES_PASSWORD="+databasePass,
		"--env", "POSTGRES_DB="+adminDatabase,
		"--publish", fmt.Sprintf("127.0.0.1:%d:5432", m.postgresPort), postgresImage).CombinedOutput()
	if err != nil {
		return fmt.Errorf("start PostgreSQL container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	var admin *pgx.Conn
	for {
		admin, err = pgx.Connect(ctx, m.databaseURL(adminDatabase))
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for PostgreSQL: %w", errors.Join(err, ctx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
	defer admin.Close(context.Background())
	for _, route := range routes {
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+route.database); err != nil {
			return fmt.Errorf("create database %s: %w", route.database, err)
		}
		connection, err := pgx.Connect(ctx, m.databaseURL(route.database))
		if err != nil {
			return fmt.Errorf("connect database %s: %w", route.database, err)
		}
		m.databases[route.kind] = connection
		if _, err := connection.Exec(ctx, `CREATE TABLE fulfillment_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    payload JSONB NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`); err != nil {
			return fmt.Errorf("create table in %s: %w", route.database, err)
		}
	}
	return nil
}

func (m *multiConsumer) cleanup() error {
	var failures []error
	if m.admin != nil {
		m.admin.Close()
	} else if m.kafkaClient != nil {
		m.kafkaClient.Close()
	}
	for kind, database := range m.databases {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := database.Close(ctx); err != nil {
			failures = append(failures, fmt.Errorf("close %s database: %w", kind, err))
		}
		cancel()
	}
	for label, name := range map[string]string{"Kafka": m.kafkaContainer, "PostgreSQL": m.postgresContainer} {
		if name == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		output, err := exec.CommandContext(ctx, "docker", "rm", "--force", name).CombinedOutput()
		cancel()
		if err != nil && !strings.Contains(string(output), "No such container") {
			failures = append(failures, fmt.Errorf("remove %s container: %w: %s", label, err, strings.TrimSpace(string(output))))
		}
	}
	m.admin, m.kafkaClient = nil, nil
	m.databases = make(map[fulfillmentType]*pgx.Conn)
	m.kafkaContainer, m.postgresContainer = "", ""
	return errors.Join(failures...)
}

func (m *multiConsumer) Prompt() (string, error) {
	if m.initErr != nil {
		return "", m.initErr
	}
	parsed, err := template.New("multi-consumer").Parse(promptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse prompt template: %w", err)
	}
	data := struct {
		KafkaBroker, KafkaUser, KafkaPassword, ConsumerGroup       string
		ShipmentTopic, PickupTopic, DigitalTopic                   string
		ShipmentDatabaseURL, PickupDatabaseURL, DigitalDatabaseURL string
	}{
		m.kafkaBroker(), kafkaUser, kafkaPassword, consumerGroup,
		shipmentTopic, pickupTopic, digitalTopic,
		m.databaseURL(shipmentDB), m.databaseURL(pickupDB), m.databaseURL(digitalDB),
	}
	var prompt bytes.Buffer
	if err := parsed.Execute(&prompt, data); err != nil {
		return "", fmt.Errorf("render prompt template: %w", err)
	}
	return prompt.String(), nil
}

func (m *multiConsumer) Feedback() (string, error) {
	if err := m.ready(); err != nil {
		return "", err
	}
	if feedback := m.checkHealth(); feedback != "" {
		return feedback, nil
	}
	result, err := m.runChecks(context.Background(), feedbackChecks())
	return result.feedback, err
}

func (m *multiConsumer) Validation() (bool, error) {
	result, err := m.runChecks(context.Background(), validationChecks())
	return result.passed, err
}

func feedbackChecks() []check {
	return []check{
		{name: "shipment route", eventID: "feedback-shipment-1042", expectedRoute: shipment, expectedStatus: http.StatusAccepted, body: `{"eventId":"feedback-shipment-1042","orderId":"ORD-2025-1042","occurredAt":"2025-03-08T10:15:30Z","customerId":"CUS-8821","fulfillmentType":"shipment","totalAmount":129.90,"currency":"USD","destination":{"country":"US","postalCode":"98101"},"items":[{"sku":"SHOE-RUN-BLK-42","quantity":1}]}`},
		{name: "pickup route", eventID: "feedback-pickup-73", expectedRoute: storePickup, expectedStatus: http.StatusAccepted, body: `{"eventId":"feedback-pickup-73","orderId":"ORD-2025-1043","occurredAt":"2025-03-08T10:17:05Z","customerId":"CUS-1940","fulfillmentType":"store_pickup","totalAmount":48.50,"currency":"GBP","storeId":"LON-017"}`},
		{name: "digital duplicate is idempotent", eventID: "feedback-digital-88", expectedRoute: digital, expectedStatus: http.StatusAccepted, repeat: 2, body: `{"eventId":"feedback-digital-88","orderId":"ORD-2025-1044","occurredAt":"2025-03-08T10:20:11Z","customerId":"CUS-7342","fulfillmentType":"digital","totalAmount":19.99,"currency":"EUR","deliveryEmail":"buyer@example.test"}`},
		{name: "unknown route rejected", eventID: "feedback-unknown", expectedStatus: http.StatusBadRequest, body: `{"eventId":"feedback-unknown","orderId":"ORD-X","occurredAt":"2025-03-08T10:20:11Z","customerId":"CUS-X","fulfillmentType":"drone","totalAmount":1,"currency":"USD"}`},
		{name: "invalid timestamp rejected", eventID: "feedback-time", expectedStatus: http.StatusBadRequest, body: `{"eventId":"feedback-time","orderId":"ORD-X","occurredAt":"yesterday","customerId":"CUS-X","fulfillmentType":"shipment","totalAmount":1,"currency":"USD"}`},
		{name: "negative amount rejected", eventID: "feedback-negative", expectedStatus: http.StatusBadRequest, body: `{"eventId":"feedback-negative","orderId":"ORD-X","occurredAt":"2025-01-01T00:00:00Z","customerId":"CUS-X","fulfillmentType":"digital","totalAmount":-0.01,"currency":"USD"}`},
		{name: "missing event ID rejected", expectedStatus: http.StatusBadRequest, body: `{"orderId":"ORD-X","occurredAt":"2025-01-01T00:00:00Z","customerId":"CUS-X","fulfillmentType":"pickup","totalAmount":1,"currency":"USD"}`},
		{name: "malformed JSON rejected", expectedStatus: http.StatusBadRequest, body: `{"eventId":"feedback-malformed"`},
		{name: "trailing JSON rejected", eventID: "feedback-trailing", expectedStatus: http.StatusBadRequest, body: `{"eventId":"feedback-trailing","orderId":"ORD-X","occurredAt":"2025-01-01T00:00:00Z","customerId":"CUS-X","fulfillmentType":"shipment","totalAmount":1,"currency":"USD"} {}`},
	}
}

func validationChecks() []check {
	return []check{
		{name: "hidden digital route", eventID: "hidden-digital-19", expectedRoute: digital, expectedStatus: http.StatusAccepted, body: `{"currency":"JPY","totalAmount":2.4e3,"fulfillmentType":"digital","customerId":"CUS-H19","occurredAt":"2026-01-19T23:41:02+09:00","orderId":"ORD-H19","eventId":"hidden-digital-19","license":{"seats":3}}`},
		{name: "hidden shipment duplicate", eventID: "hidden-shipment-42", expectedRoute: shipment, expectedStatus: http.StatusAccepted, repeat: 2, body: `{"eventId":"hidden-shipment-42","orderId":"ORD-H42","occurredAt":"2024-12-31T23:59:59Z","customerId":"CUS-H42","fulfillmentType":"shipment","totalAmount":0,"currency":"AUD","fragile":true}`},
		{name: "hidden pickup route", eventID: "hidden-pickup-7", expectedRoute: storePickup, expectedStatus: http.StatusAccepted, body: `{"eventId":"hidden-pickup-7","orderId":"ORD-H7","occurredAt":"2025-06-01T07:30:00Z","customerId":"CUS-H7","fulfillmentType":"store_pickup","totalAmount":7.05,"currency":"CAD","storeId":"YVR-2"}`},
		{name: "hidden database retry", eventID: "hidden-retry-31", expectedRoute: digital, expectedStatus: http.StatusAccepted, blockDatabase: true, body: `{"eventId":"hidden-retry-31","orderId":"ORD-H31","occurredAt":"2025-09-11T11:12:13Z","customerId":"CUS-H31","fulfillmentType":"digital","totalAmount":31,"currency":"NZD"}`},
		{name: "hidden lowercase currency rejected", eventID: "hidden-currency", expectedStatus: http.StatusBadRequest, body: `{"eventId":"hidden-currency","orderId":"ORD-X","occurredAt":"2025-01-01T00:00:00Z","customerId":"CUS-X","fulfillmentType":"shipment","totalAmount":1,"currency":"usd"}`},
		{name: "hidden nonnumeric amount rejected", eventID: "hidden-amount", expectedStatus: http.StatusBadRequest, body: `{"eventId":"hidden-amount","orderId":"ORD-X","occurredAt":"2025-01-01T00:00:00Z","customerId":"CUS-X","fulfillmentType":"digital","totalAmount":"1.00","currency":"USD"}`},
		{name: "hidden non-object rejected", expectedStatus: http.StatusBadRequest, body: `[]`},
		{name: "hidden empty customer rejected", eventID: "hidden-customer", expectedStatus: http.StatusBadRequest, body: `{"eventId":"hidden-customer","orderId":"ORD-X","occurredAt":"2025-01-01T00:00:00Z","customerId":"","fulfillmentType":"shipment","totalAmount":1,"currency":"USD"}`},
		{name: "hidden non-ASCII currency rejected", eventID: "hidden-ascii", expectedStatus: http.StatusBadRequest, body: `{"eventId":"hidden-ascii","orderId":"ORD-X","occurredAt":"2025-01-01T00:00:00Z","customerId":"CUS-X","fulfillmentType":"shipment","totalAmount":1,"currency":"ÅBC"}`},
	}
}

func (m *multiConsumer) runChecks(parent context.Context, checks []check) (checkResult, error) {
	if err := m.ready(); err != nil {
		return checkResult{}, err
	}
	ctx, cancel := context.WithTimeout(parent, checkTimeout)
	defer cancel()
	for _, current := range checks {
		result, err := m.runCheck(ctx, current)
		if err != nil || !result.passed {
			return result, err
		}
	}
	return checkResult{passed: true}, nil
}

func (m *multiConsumer) runCheck(ctx context.Context, current check) (checkResult, error) {
	if current.repeat == 0 {
		current.repeat = 1
	}
	var blockedTransaction pgx.Tx
	if current.eventID != "" {
		for _, database := range m.databases {
			if _, err := database.Exec(ctx, "DELETE FROM fulfillment_events WHERE event_id=$1", current.eventID); err != nil {
				return checkResult{}, fmt.Errorf("reset event %s: %w", current.eventID, err)
			}
		}
	}
	if current.blockDatabase {
		var err error
		blockedTransaction, err = m.databases[current.expectedRoute].Begin(ctx)
		if err != nil {
			return checkResult{}, fmt.Errorf("begin database blocking transaction: %w", err)
		}
		if _, err := blockedTransaction.Exec(ctx, "LOCK TABLE fulfillment_events IN ACCESS EXCLUSIVE MODE"); err != nil {
			_ = blockedTransaction.Rollback(context.Background())
			return checkResult{}, fmt.Errorf("lock fulfillment table: %w", err)
		}
		defer func() {
			if blockedTransaction != nil {
				_ = blockedTransaction.Rollback(context.Background())
			}
		}()
	}
	before, err := m.topicOffsets(ctx)
	if err != nil {
		return checkResult{}, err
	}
	for attempt := 0; attempt < current.repeat; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/orders", m.applicationPort), strings.NewReader(current.body))
		if err != nil {
			return checkResult{}, err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			return failure(current, fmt.Sprintf("request application: %v", err)), nil
		}
		responseBody, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			return checkResult{}, readErr
		}
		if response.StatusCode != current.expectedStatus {
			return failure(current, fmt.Sprintf("expected HTTP %d, got HTTP %d with body %q", current.expectedStatus, response.StatusCode, responseBody)), nil
		}
		if current.expectedStatus == http.StatusAccepted {
			expected := []byte(fmt.Sprintf(`{"status":"accepted","eventId":"%s"}`, current.eventID))
			equal, compareErr := jsoncompare.Equal(expected, responseBody)
			if compareErr != nil || !equal || !jsoncompare.IsJSONContentType(response.Header.Get("Content-Type")) {
				return failure(current, fmt.Sprintf("expected JSON response %s with Content-Type application/json, got %q and %q (comparison error: %v)", expected, responseBody, response.Header.Get("Content-Type"), compareErr)), nil
			}
		}
	}
	expectedDelta := make(map[string]int64)
	if current.expectedRoute != "" {
		expectedDelta[routeFor(current.expectedRoute).topic] = int64(current.repeat)
	}
	if result := m.waitForOffsets(ctx, current, before, expectedDelta); !result.passed {
		return result, nil
	}
	if current.expectedRoute != "" {
		if result, err := m.validateKafkaRecords(ctx, current, before[routeFor(current.expectedRoute).topic]); err != nil || !result.passed {
			return result, err
		}
	}
	if current.blockDatabase {
		expectedCommit := before[routeFor(current.expectedRoute).topic] + int64(current.repeat)
		if result := m.ensureOffsetUncommitted(ctx, current, expectedCommit); !result.passed {
			return result, nil
		}
		if err := blockedTransaction.Rollback(ctx); err != nil {
			return checkResult{}, fmt.Errorf("release fulfillment table lock: %w", err)
		}
		blockedTransaction = nil
	}
	if current.eventID == "" {
		return checkResult{passed: true}, nil
	}
	if current.expectedRoute == "" {
		for kind, database := range m.databases {
			count, err := eventCount(ctx, database, current.eventID)
			if err != nil {
				return checkResult{}, err
			}
			if count != 0 {
				return failure(current, fmt.Sprintf("rejected event was stored in %s database", kind)), nil
			}
		}
		return checkResult{passed: true}, nil
	}
	result, err := m.waitForDatabase(ctx, current)
	if err != nil || !result.passed {
		return result, err
	}
	return m.waitForCommittedOffset(ctx, current, before[routeFor(current.expectedRoute).topic]+int64(current.repeat)), nil
}

func (m *multiConsumer) validateKafkaRecords(ctx context.Context, current check, startOffset int64) (checkResult, error) {
	route := routeFor(current.expectedRoute)
	observer, err := kgo.NewClient(
		kgo.SeedBrokers(m.kafkaBroker()),
		kgo.SASL(plain.Auth{User: kafkaUser, Pass: kafkaPassword}.AsMechanism()),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{route.topic: {0: kgo.NewOffset().At(startOffset)}}),
	)
	if err != nil {
		return checkResult{}, fmt.Errorf("create Kafka observer: %w", err)
	}
	defer observer.Close()
	for observed := 0; observed < current.repeat; {
		fetches := observer.PollRecords(ctx, current.repeat-observed)
		if err := fetches.Err(); err != nil {
			return failure(current, fmt.Sprintf("read Kafka record: %v", err)), nil
		}
		for iterator := fetches.RecordIter(); !iterator.Done(); {
			record := iterator.Next()
			if record.Topic != route.topic || record.Partition != 0 || record.Offset < startOffset {
				continue
			}
			if string(record.Key) != current.eventID {
				return failure(current, fmt.Sprintf("Kafka record key changed: expected %q, got %q", current.eventID, record.Key)), nil
			}
			if !bytes.Equal(record.Value, []byte(current.body)) {
				return failure(current, fmt.Sprintf("Kafka record body changed: expected %q, got %q", current.body, record.Value)), nil
			}
			observed++
		}
	}
	return checkResult{passed: true}, nil
}

func (m *multiConsumer) ensureOffsetUncommitted(ctx context.Context, current check, forbidden int64) checkResult {
	deadline := time.NewTimer(6 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	topic := routeFor(current.expectedRoute).topic
	for {
		select {
		case <-ctx.Done():
			return failure(current, "timed out while checking offset commit behavior")
		case <-deadline.C:
			return checkResult{passed: true}
		case <-ticker.C:
			offsets, err := m.admin.FetchOffsets(ctx, consumerGroup)
			if err != nil {
				continue
			}
			if offset, ok := offsets.Lookup(topic, 0); ok && offset.Err == nil && offset.At >= forbidden {
				return failure(current, "consumer committed the Kafka offset before the database operation succeeded")
			}
		}
	}
}

func (m *multiConsumer) waitForCommittedOffset(ctx context.Context, current check, expected int64) checkResult {
	topic := routeFor(current.expectedRoute).topic
	for {
		offsets, err := m.admin.FetchOffsets(ctx, consumerGroup)
		if err == nil {
			if offset, ok := offsets.Lookup(topic, 0); ok && offset.Err == nil && offset.At >= expected {
				return checkResult{passed: true}
			}
		}
		select {
		case <-ctx.Done():
			return failure(current, fmt.Sprintf("consumer group did not commit %s partition 0 through offset %d", topic, expected))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (m *multiConsumer) waitForOffsets(ctx context.Context, current check, before map[string]int64, expected map[string]int64) checkResult {
	var matchedAt time.Time
	for {
		after, err := m.topicOffsets(ctx)
		if err == nil {
			matches := true
			for _, route := range routes {
				actualDelta := after[route.topic] - before[route.topic]
				expectedDelta := expected[route.topic]
				if actualDelta > expectedDelta {
					return failure(current, fmt.Sprintf("Kafka topic %s received %d records; expected %d", route.topic, actualDelta, expectedDelta))
				}
				if actualDelta != expectedDelta {
					matches = false
				}
			}
			if matches {
				if len(expected) != 0 {
					return checkResult{passed: true}
				}
				if matchedAt.IsZero() {
					matchedAt = time.Now()
				} else if time.Since(matchedAt) >= 300*time.Millisecond {
					return checkResult{passed: true}
				}
			} else {
				matchedAt = time.Time{}
			}
		}
		select {
		case <-ctx.Done():
			after, _ := m.topicOffsets(context.Background())
			return failure(current, fmt.Sprintf("Kafka topic offset changes were %v; expected %v", offsetDeltas(before, after), expected))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (m *multiConsumer) waitForDatabase(ctx context.Context, current check) (checkResult, error) {
	expectedDatabase := m.databases[current.expectedRoute]
	for {
		count, err := eventCount(ctx, expectedDatabase, current.eventID)
		if err != nil {
			return checkResult{}, err
		}
		if count == 1 {
			break
		}
		select {
		case <-ctx.Done():
			return failure(current, fmt.Sprintf("expected one row in %s database, got %d", current.expectedRoute, count)), nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	for kind, database := range m.databases {
		count, err := eventCount(ctx, database, current.eventID)
		if err != nil {
			return checkResult{}, err
		}
		if kind != current.expectedRoute && count != 0 {
			return failure(current, fmt.Sprintf("event was also stored in %s database", kind)), nil
		}
	}
	stored, err := readEvent(ctx, expectedDatabase, current.eventID)
	if err != nil {
		return checkResult{}, err
	}
	equal, err := jsoncompare.Equal([]byte(current.body), stored.payload)
	if err != nil {
		return failure(current, fmt.Sprintf("stored payload is invalid JSON: %v", err)), nil
	}
	if !equal {
		return failure(current, fmt.Sprintf("stored payload changed: got %s", stored.payload)), nil
	}
	var input map[string]any
	if err := json.Unmarshal([]byte(current.body), &input); err != nil {
		return checkResult{}, err
	}
	expectedTime, _ := time.Parse(time.RFC3339, input["occurredAt"].(string))
	expectedAmount, expectedAmountOK := new(big.Rat).SetString(fmt.Sprint(input["totalAmount"]))
	storedAmount, storedAmountOK := new(big.Rat).SetString(stored.totalAmount)
	if stored.eventID != input["eventId"] || stored.orderID != input["orderId"] || stored.customerID != input["customerId"] || !stored.occurredAt.Equal(expectedTime) || stored.currency != input["currency"] || !expectedAmountOK || !storedAmountOK || expectedAmount.Cmp(storedAmount) != 0 {
		return failure(current, "stored indexed columns do not match the request"), nil
	}
	return checkResult{passed: true}, nil
}

func (m *multiConsumer) topicOffsets(ctx context.Context) (map[string]int64, error) {
	listed, err := m.admin.ListEndOffsets(ctx, shipmentTopic, pickupTopic, digitalTopic)
	if err != nil {
		return nil, fmt.Errorf("list Kafka offsets: %w", err)
	}
	offsets := make(map[string]int64, len(routes))
	for _, route := range routes {
		partitions := listed[route.topic]
		if partition, ok := partitions[0]; ok {
			if partition.Err != nil {
				return nil, fmt.Errorf("list offset for %s: %w", route.topic, partition.Err)
			}
			offsets[route.topic] = partition.Offset
		}
	}
	return offsets, nil
}

func eventCount(ctx context.Context, database *pgx.Conn, eventID string) (int, error) {
	var count int
	err := database.QueryRow(ctx, "SELECT count(*) FROM fulfillment_events WHERE event_id=$1", eventID).Scan(&count)
	return count, err
}

func readEvent(ctx context.Context, database *pgx.Conn, eventID string) (storedEvent, error) {
	var event storedEvent
	err := database.QueryRow(ctx, `SELECT event_id, order_id, occurred_at, customer_id, total_amount::text, currency, payload FROM fulfillment_events WHERE event_id=$1`, eventID).Scan(
		&event.eventID, &event.orderID, &event.occurredAt, &event.customerID, &event.totalAmount, &event.currency, &event.payload)
	return event, err
}

func (m *multiConsumer) checkHealth() string {
	response, err := (&http.Client{Timeout: 5 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/health", m.applicationPort))
	if err != nil {
		return fmt.Sprintf("Check %q failed. Diagnosis: GET /health failed: %v", "application health", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Sprintf("Check %q failed. Expected a 2xx response, got HTTP %d.", "application health", response.StatusCode)
	}
	return ""
}

func (m *multiConsumer) ready() error {
	if m.initErr != nil {
		return m.initErr
	}
	if m.kafkaClient == nil || m.admin == nil || len(m.databases) != len(routes) {
		return errors.New("multi consumer infrastructure is not running")
	}
	return nil
}

func (m *multiConsumer) kafkaBroker() string { return fmt.Sprintf("127.0.0.1:%d", m.kafkaPort) }

func (m *multiConsumer) databaseURL(database string) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", databaseUser, databasePass, m.postgresPort, database)
}

func routeFor(kind fulfillmentType) routeSpec {
	for _, route := range routes {
		if route.kind == kind {
			return route
		}
	}
	return routeSpec{}
}

func offsetDeltas(before, after map[string]int64) map[string]int64 {
	deltas := make(map[string]int64, len(routes))
	for _, route := range routes {
		deltas[route.topic] = after[route.topic] - before[route.topic]
	}
	return deltas
}

func failure(current check, diagnosis string) checkResult {
	return checkResult{feedback: fmt.Sprintf("Check %q failed. Request: POST /orders body=%q. Diagnosis: %s", current.name, current.body, diagnosis)}
}
