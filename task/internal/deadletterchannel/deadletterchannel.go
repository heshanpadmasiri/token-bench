// Package deadletterchannel implements the Dead Letter Channel task with a
// Dockerized RabbitMQ broker owned by the benchmark.
package deadletterchannel

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"text/template"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	requiredPorts      = 2
	rabbitMQImage      = "rabbitmq:3.13-alpine"
	mainQueue          = "messages"
	deadLetterQueue    = "dead-letters"
	deadLetterExchange = "token-bench.dead-letters"
)

//go:embed prompt.md.tmpl
var promptTemplate string

type deadLetterChannel struct {
	brokerPort    int
	containerName string
	connection    *amqp.Connection
	channel       *amqp.Channel
	initErr       error
}

type testMessage struct {
	MessageID string         `json:"messageId"`
	Payload   map[string]any `json:"payload"`
}

type checkResult struct {
	passed   bool
	feedback string
}

type brokerQueueState struct {
	Name                   string `json:"name"`
	MessagesReady          int    `json:"messages_ready"`
	MessagesUnacknowledged int    `json:"messages_unacknowledged"`
	Consumers              int    `json:"consumers"`
}

// PromptTemplate returns the unrendered task requirements.
func PromptTemplate() string {
	return promptTemplate
}

// New initializes the private Dead Letter Channel implementation.
func New(ports []int) *deadLetterChannel {
	handle := &deadLetterChannel{}
	if len(ports) != requiredPorts {
		handle.initErr = fmt.Errorf("dead letter channel requires %d ports, got %d", requiredPorts, len(ports))
		return handle
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			handle.initErr = fmt.Errorf("invalid allocated port %d", port)
			return handle
		}
	}
	handle.brokerPort = ports[1]
	return handle
}

func (d *deadLetterChannel) Setup() (func() error, error) {
	if d.initErr != nil {
		return nil, d.initErr
	}
	if d.containerName != "" {
		return nil, errors.New("dead letter channel is already set up")
	}
	d.containerName = fmt.Sprintf("token-bench-rabbitmq-%d-%d", d.brokerPort, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm", "--name", d.containerName,
		"--publish", fmt.Sprintf("127.0.0.1:%d:5672", d.brokerPort), rabbitMQImage).CombinedOutput()
	if err != nil {
		d.containerName = ""
		return nil, fmt.Errorf("start RabbitMQ container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := d.connectAndConfigure(ctx); err != nil {
		_ = d.cleanup()
		return nil, err
	}
	messages := append(feedbackMessages(), validationMessages()...)
	if err := d.publish(ctx, messages); err != nil {
		_ = d.cleanup()
		return nil, err
	}
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() { cleanupErr = d.cleanup() })
		return cleanupErr
	}, nil
}

func (d *deadLetterChannel) connectAndConfigure(ctx context.Context) error {
	var err error
	for {
		d.connection, err = amqp.DialConfig(d.brokerURL(), amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for RabbitMQ: %w", errors.Join(err, ctx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
	d.channel, err = d.connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	if err := d.channel.ExchangeDeclare(deadLetterExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead letter exchange: %w", err)
	}
	if _, err := d.channel.QueueDeclare(deadLetterQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead letter queue: %w", err)
	}
	if err := d.channel.QueueBind(deadLetterQueue, deadLetterQueue, deadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("bind dead letter queue: %w", err)
	}
	arguments := amqp.Table{"x-dead-letter-exchange": deadLetterExchange, "x-dead-letter-routing-key": deadLetterQueue}
	if _, err := d.channel.QueueDeclare(mainQueue, true, false, false, false, arguments); err != nil {
		return fmt.Errorf("declare source queue: %w", err)
	}
	if err := d.channel.Confirm(false); err != nil {
		return fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	return nil
}

func (d *deadLetterChannel) cleanup() error {
	var failures []error
	if d.connection != nil {
		if err := d.connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			failures = append(failures, fmt.Errorf("close RabbitMQ connection: %w", err))
		}
	}
	if d.containerName != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "docker", "rm", "--force", d.containerName).CombinedOutput()
		if err != nil && !strings.Contains(string(output), "No such container") {
			failures = append(failures, fmt.Errorf("remove RabbitMQ container: %w: %s", err, strings.TrimSpace(string(output))))
		}
	}
	d.connection, d.channel, d.containerName = nil, nil, ""
	return errors.Join(failures...)
}

func (d *deadLetterChannel) Prompt() (string, error) {
	if d.initErr != nil {
		return "", d.initErr
	}
	parsed, err := template.New("dead-letter-channel").Parse(promptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse prompt template: %w", err)
	}
	var prompt bytes.Buffer
	data := struct{ BrokerURL, Queue string }{d.brokerURL(), mainQueue}
	if err := parsed.Execute(&prompt, data); err != nil {
		return "", fmt.Errorf("render prompt template: %w", err)
	}
	return prompt.String(), nil
}

func feedbackMessages() []testMessage {
	return []testMessage{
		{MessageID: "message-1042", Payload: map[string]any{"amount": 42}},
		{MessageID: "failed-73", Payload: map[string]any{"amount": -73, "items": []any{1, 2, 3}}},
		{MessageID: "message-1043", Payload: map[string]any{"amount": 0}},
	}
}

func validationMessages() []testMessage {
	return []testMessage{
		{MessageID: "hidden-success", Payload: map[string]any{"amount": 19.95, "hidden": true}},
		{MessageID: "hidden-failure-1", Payload: map[string]any{"amount": -0.01, "note": "reject me"}},
		{MessageID: "hidden-failure-2", Payload: map[string]any{"amount": -1042}},
	}
}

func (d *deadLetterChannel) Feedback() (string, error) {
	result, err := d.runCheck(context.Background(), feedbackMessages(), 3)
	return result.feedback, err
}

func (d *deadLetterChannel) Validation() (bool, error) {
	result, err := d.runCheck(context.Background(), validationMessages(), 3)
	return result.passed, err
}

func (d *deadLetterChannel) publish(ctx context.Context, messages []testMessage) error {
	confirmations := d.channel.NotifyPublish(make(chan amqp.Confirmation, len(messages)))
	for _, message := range messages {
		body, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("encode test message: %w", err)
		}
		publishing := amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, CorrelationId: message.MessageID, Body: body}
		if err := d.channel.PublishWithContext(ctx, "", mainQueue, false, false, publishing); err != nil {
			return fmt.Errorf("publish %s to source queue: %w", message.MessageID, err)
		}
	}
	for range messages {
		select {
		case confirmation := <-confirmations:
			if !confirmation.Ack {
				return errors.New("RabbitMQ rejected a benchmark message")
			}
		case <-ctx.Done():
			return fmt.Errorf("wait for RabbitMQ publisher confirmation: %w", ctx.Err())
		}
	}
	return nil
}

func (d *deadLetterChannel) runCheck(parent context.Context, messages []testMessage, totalDeadLetters int) (checkResult, error) {
	if err := d.ready(); err != nil {
		return checkResult{}, err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	expectedDeadLetters := make(map[string][]byte)
	for _, message := range messages {
		body, err := json.Marshal(message)
		if err != nil {
			return checkResult{}, fmt.Errorf("encode test message: %w", err)
		}
		if amount, ok := numericAmount(message.Payload["amount"]); ok && amount < 0 {
			expectedDeadLetters[message.MessageID] = body
		}
	}

	var sourceState, deadState amqp.Queue
	for {
		var err error
		sourceState, err = d.channel.QueueInspect(mainQueue)
		if err != nil {
			return checkResult{}, fmt.Errorf("inspect source queue: %w", err)
		}
		deadState, err = d.channel.QueueInspect(deadLetterQueue)
		if err != nil {
			return checkResult{}, fmt.Errorf("inspect dead letter queue: %w", err)
		}
		if sourceState.Messages == 0 && deadState.Messages == totalDeadLetters {
			states, stateErr := d.queueStates(ctx)
			if stateErr != nil {
				return checkResult{}, stateErr
			}
			if states[mainQueue].MessagesReady == 0 && states[mainQueue].MessagesUnacknowledged == 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return checkResult{feedback: fmt.Sprintf("The source queue was not fully consumed or the dead letter queue was not populated. Source messages=%d consumers=%d; dead letter messages=%d, expected=%d.", sourceState.Messages, sourceState.Consumers, deadState.Messages, totalDeadLetters)}, nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	if sourceState.Consumers == 0 {
		return checkResult{feedback: "The source queue has no active consumer."}, nil
	}

	retrieved := make([]amqp.Delivery, 0, totalDeadLetters)
	for index := 0; index < totalDeadLetters; index++ {
		delivery, ok, err := d.channel.Get(deadLetterQueue, false)
		if err != nil {
			d.requeue(retrieved)
			return checkResult{}, fmt.Errorf("read dead letter queue: %w", err)
		}
		if !ok {
			d.requeue(retrieved)
			return checkResult{feedback: "The dead letter queue became empty before all rejected messages were validated."}, nil
		}
		retrieved = append(retrieved, delivery)
		expectedBody, expected := expectedDeadLetters[delivery.CorrelationId]
		if !expected {
			continue
		}
		if !bytes.Equal(delivery.Body, expectedBody) {
			d.requeue(retrieved)
			return checkResult{feedback: fmt.Sprintf("Dead-lettered message %q had a modified body.", delivery.CorrelationId)}, nil
		}
		if _, exists := delivery.Headers["x-death"]; !exists {
			d.requeue(retrieved)
			return checkResult{feedback: fmt.Sprintf("Message %q lacks RabbitMQ x-death metadata; it may have been published directly to the dead letter queue.", delivery.CorrelationId)}, nil
		}
		delete(expectedDeadLetters, delivery.CorrelationId)
	}
	if len(expectedDeadLetters) != 0 {
		d.requeue(retrieved)
		return checkResult{feedback: "Expected rejected messages were missing from the dead letter queue."}, nil
	}
	if err := d.requeue(retrieved); err != nil {
		return checkResult{}, fmt.Errorf("restore inspected dead letters: %w", err)
	}
	return checkResult{passed: true}, nil
}

func (d *deadLetterChannel) requeue(deliveries []amqp.Delivery) error {
	var failures []error
	for _, delivery := range deliveries {
		if err := delivery.Nack(false, true); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (d *deadLetterChannel) queueStates(ctx context.Context) (map[string]brokerQueueState, error) {
	output, err := exec.CommandContext(ctx, "docker", "exec", d.containerName, "rabbitmqctl", "list_queues", "--formatter", "json", "name", "messages_ready", "messages_unacknowledged", "consumers").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("inspect RabbitMQ queues: %w: %s", err, strings.TrimSpace(string(output)))
	}
	var listed []brokerQueueState
	if err := json.Unmarshal(output, &listed); err != nil {
		return nil, fmt.Errorf("decode RabbitMQ queue state %q: %w", output, err)
	}
	states := make(map[string]brokerQueueState, len(listed))
	for _, state := range listed {
		states[state.Name] = state
	}
	return states, nil
}

func numericAmount(value any) (float64, bool) {
	switch amount := value.(type) {
	case int:
		return float64(amount), true
	case float64:
		return amount, true
	default:
		return 0, false
	}
}

func (d *deadLetterChannel) ready() error {
	if d.initErr != nil {
		return d.initErr
	}
	if d.connection == nil || d.connection.IsClosed() {
		return errors.New("RabbitMQ backend is not running")
	}
	return nil
}

func (d *deadLetterChannel) brokerURL() string {
	return fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", d.brokerPort)
}
