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
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"text/template"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	requiredPorts          = 2
	rabbitMQImage          = "rabbitmq:3.13-alpine"
	mainQueue              = "messages"
	deadLetterQueue        = "dead-letters"
	deadLetterExchange     = "token-bench.dead-letters"
	connectionCloseTimeout = 2 * time.Second
)

//go:embed prompt.md.tmpl
var promptTemplate string

type rabbitMQConnection interface {
	Channel() (*amqp.Channel, error)
	CloseDeadline(time.Time) error
	IsClosed() bool
}

type deadLetterChannel struct {
	applicationPort int
	brokerPort      int
	containerName   string
	connection      rabbitMQConnection
	channel         *amqp.Channel
	confirmations   <-chan amqp.Confirmation
	initErr         error
}

type queueMessage struct {
	messageID string
	body      []byte
	rejected  bool
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
	handle.applicationPort, handle.brokerPort = ports[0], ports[1]
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
	d.confirmations = d.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	return nil
}

func (d *deadLetterChannel) cleanup() error {
	var failures []error
	if d.connection != nil {
		deadline := time.Now().Add(connectionCloseTimeout)
		if err := d.connection.CloseDeadline(deadline); err != nil && !errors.Is(err, amqp.ErrClosed) {
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
	d.connection, d.channel, d.confirmations, d.containerName = nil, nil, nil, ""
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

func feedbackMessages() []queueMessage {
	return []queueMessage{
		{messageID: "message-1042", body: []byte(`{"messageId":"message-1042","payload":{"amount":42}}`)},
		{messageID: "failed-73", body: []byte(`{"messageId":"failed-73","payload":{"amount":-73,"items":[1,2,3]}}`), rejected: true},
		{messageID: "message-1043", body: []byte(`{"messageId":"message-1043","payload":{"amount":0.0}}`)},
		{messageID: "feedback-malformed", body: []byte(`{"messageId":"feedback-malformed"`), rejected: true},
		{messageID: "feedback-missing-amount", body: []byte(`{"messageId":"feedback-missing-amount","payload":{}}`), rejected: true},
		{messageID: "feedback-string-amount", body: []byte(`{"messageId":"feedback-string-amount","payload":{"amount":"-1"}}`), rejected: true},
	}
}

func validationMessages() []queueMessage {
	return []queueMessage{
		{messageID: "hidden-success", body: []byte(`{"payload":{"hidden":true,"amount":1.995e1},"messageId":"hidden-success"}`)},
		{messageID: "hidden-failure-1", body: []byte(`{"messageId":"hidden-failure-1","payload":{"note":"reject me","amount":-1e-2}}`), rejected: true},
		{messageID: "hidden-failure-2", body: []byte(`{"messageId":"hidden-failure-2","payload":{"amount":-1042.0}}`), rejected: true},
		{messageID: "hidden-null-amount", body: []byte(`{"messageId":"hidden-null-amount","payload":{"amount":null}}`), rejected: true},
		{messageID: "hidden-array", body: []byte(`[]`), rejected: true},
	}
}

func (d *deadLetterChannel) Feedback() (string, error) {
	if err := d.ready(); err != nil {
		return "", err
	}
	if feedback := d.checkHealth(); feedback != "" {
		return feedback, nil
	}
	result, err := d.runCheck(context.Background(), feedbackMessages())
	return result.feedback, err
}

func (d *deadLetterChannel) Validation() (bool, error) {
	result, err := d.runCheck(context.Background(), validationMessages())
	return result.passed, err
}

func (d *deadLetterChannel) publish(ctx context.Context, messages []queueMessage) error {
	for _, message := range messages {
		publishing := amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, CorrelationId: message.messageID, Body: message.body}
		if err := d.channel.PublishWithContext(ctx, "", mainQueue, false, false, publishing); err != nil {
			return fmt.Errorf("publish %s to source queue: %w", message.messageID, err)
		}
		select {
		case confirmation, ok := <-d.confirmations:
			if !ok {
				return errors.New("RabbitMQ publisher confirmation channel closed")
			}
			if !confirmation.Ack {
				return errors.New("RabbitMQ rejected a benchmark message")
			}
		case <-ctx.Done():
			return fmt.Errorf("wait for RabbitMQ publisher confirmation: %w", ctx.Err())
		}
	}
	return nil
}

func (d *deadLetterChannel) runCheck(parent context.Context, messages []queueMessage) (checkResult, error) {
	if err := d.ready(); err != nil {
		return checkResult{}, err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if err := d.resetQueues(ctx); err != nil {
		return checkResult{}, err
	}
	expectedDeadLetters := make(map[string][]byte)
	for _, message := range messages {
		if message.rejected {
			expectedDeadLetters[message.messageID] = message.body
		}
	}
	totalDeadLetters := len(expectedDeadLetters)
	if err := d.publish(ctx, messages); err != nil {
		return checkResult{}, err
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

func (d *deadLetterChannel) resetQueues(ctx context.Context) error {
	for {
		if _, err := d.channel.QueuePurge(mainQueue, false); err != nil {
			return fmt.Errorf("reset source queue: %w", err)
		}
		if _, err := d.channel.QueuePurge(deadLetterQueue, false); err != nil {
			return fmt.Errorf("reset dead letter queue: %w", err)
		}
		states, err := d.queueStates(ctx)
		if err != nil {
			return err
		}
		if states[mainQueue].MessagesReady == 0 && states[mainQueue].MessagesUnacknowledged == 0 {
			_, err := d.channel.QueuePurge(deadLetterQueue, false)
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait to reset queues: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (d *deadLetterChannel) checkHealth() string {
	response, err := (&http.Client{Timeout: 5 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/health", d.applicationPort))
	if err != nil {
		return fmt.Sprintf("Check %q failed. Diagnosis: GET /health failed: %v", "consumer health", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Sprintf("Check %q failed. Expected a 2xx response, got HTTP %d.", "consumer health", response.StatusCode)
	}
	return ""
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
