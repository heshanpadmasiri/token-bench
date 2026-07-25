// Package claimcheck implements the Claim Check task with Dockerized Redis
// and benchmark-owned intermediary and final HTTP backends.
package claimcheck

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/redis/go-redis/v9"

	"token-bench/task/internal/jsoncompare"
)

const (
	requiredPorts = 5
	redisImage    = "redis:7.4-alpine"
	claimTTL      = 5 * time.Minute
)

//go:embed prompt.md.tmpl
var promptTemplate string

type claimCheck struct {
	serviceAPort     int
	serviceBPort     int
	redisPort        int
	intermediaryPort int
	finalPort        int
	containerName    string
	redis            *redis.Client
	intermediary     *intermediaryBackend
	final            *recordingBackend
	initErr          error
}

type address struct {
	Street     string `json:"street"`
	City       string `json:"city"`
	State      string `json:"state"`
	PostalCode string `json:"postalCode"`
	Country    string `json:"country"`
}

type observedRequest struct {
	method string
	path   string
	body   []byte
}

type recordingBackend struct {
	server   *http.Server
	listener net.Listener
	mu       sync.Mutex
	status   int
	response string
	requests []observedRequest
}

type intermediaryBackend struct {
	server      *http.Server
	listener    net.Listener
	redis       *redis.Client
	serviceBURL string
	mu          sync.Mutex
	status      int
	response    string
	delivery    address
	expectedID  string
	requests    []observedRequest
	messageIDs  []string
}

type check struct {
	name                 string
	body                 string
	repeat               int
	expectedStatus       int
	expectedResponse     string
	expectedUserID       string
	delivery             address
	intermediaryStatus   int
	intermediaryResponse string
	finalStatus          int
	finalResponse        string
	expectedFinal        map[string]any
	expectIntermediary   bool
	expectFinal          bool
	expectClaimRetained  bool
}

type checkResult struct {
	passed   bool
	feedback string
}

// PromptTemplate returns the unrendered task requirements.
func PromptTemplate() string {
	return promptTemplate
}

// New initializes the private Claim Check implementation.
func New(ports []int) *claimCheck {
	handle := &claimCheck{}
	if len(ports) != requiredPorts {
		handle.initErr = fmt.Errorf("claim check requires %d ports, got %d", requiredPorts, len(ports))
		return handle
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			handle.initErr = fmt.Errorf("invalid allocated port %d", port)
			return handle
		}
	}
	handle.serviceAPort, handle.serviceBPort = ports[0], ports[1]
	handle.redisPort, handle.intermediaryPort, handle.finalPort = ports[2], ports[3], ports[4]
	return handle
}

func (c *claimCheck) Setup() (func() error, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	if c.containerName != "" || c.intermediary != nil || c.final != nil {
		return nil, errors.New("claim check is already set up")
	}
	c.containerName = fmt.Sprintf("token-bench-redis-%d-%d", c.redisPort, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm", "--name", c.containerName,
		"--publish", fmt.Sprintf("127.0.0.1:%d:6379", c.redisPort), redisImage).CombinedOutput()
	if err != nil {
		c.containerName = ""
		return nil, fmt.Errorf("start Redis container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	c.redis = redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", c.redisPort)})
	for {
		err = c.redis.Ping(ctx).Err()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			_ = c.cleanup()
			return nil, fmt.Errorf("wait for Redis: %w", errors.Join(err, ctx.Err()))
		case <-time.After(200 * time.Millisecond):
		}
	}
	c.final, err = startRecordingBackend(c.finalPort)
	if err != nil {
		_ = c.cleanup()
		return nil, fmt.Errorf("start final backend: %w", err)
	}
	c.intermediary, err = startIntermediary(c.intermediaryPort, c.redis, origin(c.serviceBPort)+"/messages")
	if err != nil {
		_ = c.cleanup()
		return nil, fmt.Errorf("start intermediary backend: %w", err)
	}
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() { cleanupErr = c.cleanup() })
		return cleanupErr
	}, nil
}

func (c *claimCheck) cleanup() error {
	var failures []error
	if c.intermediary != nil {
		failures = append(failures, c.intermediary.stop())
	}
	if c.final != nil {
		failures = append(failures, c.final.stop())
	}
	if c.redis != nil {
		failures = append(failures, c.redis.Close())
	}
	if c.containerName != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		output, err := exec.CommandContext(ctx, "docker", "rm", "--force", c.containerName).CombinedOutput()
		cancel()
		if err != nil && !strings.Contains(string(output), "No such container") {
			failures = append(failures, fmt.Errorf("remove Redis container: %w: %s", err, strings.TrimSpace(string(output))))
		}
	}
	c.intermediary, c.final, c.redis, c.containerName = nil, nil, nil, ""
	return errors.Join(failures...)
}

func (c *claimCheck) Prompt() (string, error) {
	if c.initErr != nil {
		return "", c.initErr
	}
	parsed, err := template.New("claim-check").Parse(promptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse prompt template: %w", err)
	}
	data := struct {
		ServiceAPort, ServiceBPort, RedisPort int
		IntermediaryURL, FinalURL             string
	}{c.serviceAPort, c.serviceBPort, c.redisPort, origin(c.intermediaryPort) + "/claimed-messages", origin(c.finalPort) + "/deliveries"}
	var prompt bytes.Buffer
	if err := parsed.Execute(&prompt, data); err != nil {
		return "", fmt.Errorf("render prompt template: %w", err)
	}
	return prompt.String(), nil
}

func feedbackDelivery() address {
	return address{"500 Delivery Road", "Edinburgh", "Scotland", "EH1 1AA", "GB"}
}

func hiddenDelivery() address {
	return address{"8 Harbour Road", "Sydney", "NSW", "2000", "AU"}
}

func (c *claimCheck) Feedback() (string, error) {
	if err := c.ready(); err != nil {
		return "", err
	}
	if feedback := c.checkHealth(); feedback != "" {
		return feedback, nil
	}
	delivery := feedbackDelivery()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := c.runChecks(ctx, []check{
		{name: "claims and restores user ID", body: `{"userId":"user-1042","userName":"Ada Lovelace","userAddress":{"street":"12 Example Street","city":"London","state":"Greater London","postalCode":"SW1A 1AA","country":"GB"},"metadata":{"attempt":1,"active":true}}`, repeat: 2, expectedStatus: http.StatusAccepted, expectedUserID: "user-1042", delivery: delivery, finalStatus: http.StatusCreated, finalResponse: `created`, expectedFinal: message("user-1042", "Ada Lovelace", address{"12 Example Street", "London", "Greater London", "SW1A 1AA", "GB"}, delivery, map[string]any{"metadata": map[string]any{"attempt": float64(1), "active": true}}), expectIntermediary: true, expectFinal: true},
		{name: "final failure is relayed and retains claim", body: `{"userId":"user-73","userName":"Grace Hopper","userAddress":{"street":"73 Market Avenue","city":"Arlington","state":"VA","postalCode":"22201","country":"US"}}`, expectedStatus: http.StatusServiceUnavailable, expectedResponse: `{"error":"unavailable"}`, expectedUserID: "user-73", delivery: delivery, finalStatus: http.StatusServiceUnavailable, finalResponse: `{ "error" : "unavailable" }`, expectedFinal: message("user-73", "Grace Hopper", address{"73 Market Avenue", "Arlington", "VA", "22201", "US"}, delivery, nil), expectIntermediary: true, expectFinal: true, expectClaimRetained: true},
		{name: "intermediary rejection is relayed and retains claim", body: `{"userId":"user-9","userName":"Blocked User","userAddress":{"street":"9 Main Street","city":"Boston","state":"MA","postalCode":"02108","country":"US"}}`, expectedStatus: http.StatusConflict, expectedResponse: `blocked`, expectedUserID: "user-9", delivery: delivery, intermediaryStatus: http.StatusConflict, intermediaryResponse: `blocked`, finalStatus: http.StatusOK, expectIntermediary: true, expectClaimRetained: true},
		{name: "unreachable intermediary returns 502", body: `{"userId":"user-unreachable-a","userName":"Retry User","userAddress":{"street":"1 Main","city":"Boston","state":"MA","postalCode":"02108","country":"US"}}`, expectedStatus: http.StatusBadGateway, expectedUserID: "user-unreachable-a", delivery: delivery, intermediaryStatus: -1, finalStatus: http.StatusOK, expectIntermediary: true, expectClaimRetained: true},
		{name: "unreachable final returns 502", body: `{"userId":"user-unreachable-b","userName":"Retry User","userAddress":{"street":"2 Main","city":"Boston","state":"MA","postalCode":"02108","country":"US"}}`, expectedStatus: http.StatusBadGateway, expectedUserID: "user-unreachable-b", delivery: delivery, finalStatus: -1, expectedFinal: message("user-unreachable-b", "Retry User", address{"2 Main", "Boston", "MA", "02108", "US"}, delivery, nil), expectIntermediary: true, expectFinal: true, expectClaimRetained: true},
		{name: "malformed JSON", body: `{"userId":"user-1"`, expectedStatus: http.StatusBadRequest},
		{name: "missing user name", body: `{"userId":"user-1","userAddress":{"street":"1 A","city":"B","state":"C","postalCode":"D","country":"E"}}`, expectedStatus: http.StatusBadRequest},
		{name: "invalid address", body: `{"userId":"user-1","userName":"Name","userAddress":{"street":"","city":"B","state":"C","postalCode":"D","country":"E"}}`, expectedStatus: http.StatusBadRequest},
		{name: "reserved message ID", body: `{"userId":"user-1","userName":"Name","messageId":"caller-value","userAddress":{"street":"A","city":"B","state":"C","postalCode":"D","country":"E"}}`, expectedStatus: http.StatusBadRequest},
		{name: "trailing JSON", body: `{"userId":"user-1","userName":"Name","userAddress":{"street":"A","city":"B","state":"C","postalCode":"D","country":"E"}} {}`, expectedStatus: http.StatusBadRequest},
		{name: "non-object JSON", body: `[]`, expectedStatus: http.StatusBadRequest},
	})
	if !result.passed {
		return result.feedback, nil
	}
	return c.checkServiceBErrors(ctx, false), nil
}

func (c *claimCheck) Validation() (bool, error) {
	if err := c.ready(); err != nil {
		return false, err
	}
	delivery := hiddenDelivery()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := c.runChecks(ctx, []check{
		{name: "hidden nested fields", body: `{"userName":"Hidden User","userAddress":{"country":"JP","postalCode":"600-8216","state":"Kyoto","city":"Kyoto","street":"4-2 Sakura Lane","extra":{"floor":2}},"userId":"hidden-user-88","payload":{"amount":1.995e1,"tags":["a",2.0],"nullable":null}}`, expectedStatus: http.StatusAccepted, expectedUserID: "hidden-user-88", delivery: delivery, finalStatus: http.StatusNoContent, expectedFinal: message("hidden-user-88", "Hidden User", address{"4-2 Sakura Lane", "Kyoto", "Kyoto", "600-8216", "JP"}, delivery, map[string]any{"payload": map[string]any{"amount": 19.95, "tags": []any{"a", float64(2)}, "nullable": nil}, "userAddressExtra": map[string]any{"extra": map[string]any{"floor": float64(2)}}}), expectIntermediary: true, expectFinal: true},
		{name: "hidden existing delivery address", body: `{"userId":"hidden","userName":"Hidden","userAddress":{"street":"A","city":"B","state":"C","postalCode":"D","country":"E"},"deliveryAddress":null}`, expectedStatus: http.StatusBadRequest},
		{name: "hidden non-string user ID", body: `{"userId":88,"userName":"Hidden","userAddress":{"street":"A","city":"B","state":"C","postalCode":"D","country":"E"}}`, expectedStatus: http.StatusBadRequest},
	})
	if !result.passed {
		return false, nil
	}
	return c.checkServiceBErrors(ctx, true) == "", nil
}

func message(userID, userName string, userAddress, delivery address, extras map[string]any) map[string]any {
	result := map[string]any{"userId": userID, "userName": userName, "userAddress": userAddress, "deliveryAddress": delivery}
	for key, value := range extras {
		if key == "userAddressExtra" {
			current := map[string]any{"street": userAddress.Street, "city": userAddress.City, "state": userAddress.State, "postalCode": userAddress.PostalCode, "country": userAddress.Country}
			for nestedKey, nestedValue := range value.(map[string]any) {
				current[nestedKey] = nestedValue
			}
			result["userAddress"] = current
			continue
		}
		result[key] = value
	}
	return result
}

func (c *claimCheck) runChecks(ctx context.Context, checks []check) checkResult {
	for _, current := range checks {
		if result := c.runCheck(ctx, current); !result.passed {
			return result
		}
	}
	return checkResult{passed: true}
}

func (c *claimCheck) runCheck(ctx context.Context, current check) checkResult {
	if err := c.clearClaims(ctx); err != nil {
		return failure(current, fmt.Sprintf("clear benchmark claims: %v", err))
	}
	c.intermediary.configure(current.intermediaryStatus, current.intermediaryResponse, current.expectedUserID, current.delivery)
	c.final.configure(current.finalStatus, current.finalResponse)
	repeat := current.repeat
	if repeat == 0 {
		repeat = 1
	}
	for index := 0; index < repeat; index++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, origin(c.serviceAPort)+"/messages", strings.NewReader(current.body))
		if err != nil {
			return failure(current, fmt.Sprintf("construct request: %v", err))
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			return failure(current, fmt.Sprintf("request service A: %v", err))
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			return failure(current, fmt.Sprintf("read response: %v", readErr))
		}
		if response.StatusCode != current.expectedStatus {
			return failure(current, fmt.Sprintf("expected HTTP %d body %q, got HTTP %d body %q", current.expectedStatus, current.expectedResponse, response.StatusCode, body))
		}
		if current.expectedStatus == http.StatusAccepted || current.expectedResponse != "" {
			responseMatches, compareErr := jsoncompare.EqualResponse([]byte(current.expectedResponse), body)
			if compareErr != nil || !responseMatches {
				return failure(current, fmt.Sprintf("expected body %q, got %q (comparison error: %v)", current.expectedResponse, body, compareErr))
			}
		}
	}
	intermediaryRequests, messageIDs := c.intermediary.snapshot()
	expectedIntermediaryCount := 0
	if current.expectIntermediary {
		expectedIntermediaryCount = repeat
	}
	if len(intermediaryRequests) != expectedIntermediaryCount {
		return failure(current, fmt.Sprintf("expected %d intermediary request(s), got %d", expectedIntermediaryCount, len(intermediaryRequests)))
	}
	finalRequests := c.final.snapshot()
	expectedFinalCount := 0
	if current.expectFinal {
		expectedFinalCount = repeat
	}
	if len(finalRequests) != expectedFinalCount {
		return failure(current, fmt.Sprintf("expected %d final request(s), got %d", expectedFinalCount, len(finalRequests)))
	}
	if len(messageIDs) > 1 {
		seen := map[string]bool{}
		for _, id := range messageIDs {
			if seen[id] {
				return failure(current, "service A reused a message ID")
			}
			seen[id] = true
		}
	}
	for _, observed := range finalRequests {
		if observed.method != http.MethodPost || observed.path != "/deliveries" {
			return failure(current, fmt.Sprintf("final endpoint received method=%q path=%q", observed.method, observed.path))
		}
		if current.expectedFinal != nil {
			expected, err := json.Marshal(current.expectedFinal)
			equal, compareErr := jsoncompare.Equal(expected, observed.body)
			if err != nil || compareErr != nil || !equal {
				return failure(current, fmt.Sprintf("final payload was not restored and preserved: got %s (comparison error: %v)", observed.body, errors.Join(err, compareErr)))
			}
		}
	}
	if !current.expectIntermediary {
		size, err := c.redis.DBSize(ctx).Result()
		if err != nil || size != 0 {
			return failure(current, fmt.Sprintf("invalid input changed Redis: keys=%d error=%v", size, err))
		}
	}
	for _, id := range messageIDs {
		value, err := c.redis.Get(ctx, "claim:"+id).Result()
		if current.expectClaimRetained {
			if err != nil || value != current.expectedUserID {
				return failure(current, fmt.Sprintf("claim was not retained after failure: value=%q error=%v", value, err))
			}
			ttl, err := c.redis.TTL(ctx, "claim:"+id).Result()
			if err != nil || ttl < claimTTL-30*time.Second || ttl > claimTTL {
				return failure(current, fmt.Sprintf("claim TTL is invalid: %v (error %v)", ttl, err))
			}
		} else if current.expectFinal && current.finalStatus >= 200 && current.finalStatus < 300 && !errors.Is(err, redis.Nil) {
			return failure(current, "claim was not deleted after successful final delivery")
		}
	}
	return checkResult{passed: true}
}

func (c *claimCheck) checkHealth() string {
	client := &http.Client{Timeout: 5 * time.Second}
	for name, port := range map[string]int{"Service A": c.serviceAPort, "Service B": c.serviceBPort} {
		response, err := client.Get(origin(port) + "/health")
		if err != nil {
			return fmt.Sprintf("Check %q failed. Diagnosis: GET /health failed: %v", name+" health", err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Sprintf("Check %q failed. Expected a 2xx response, got HTTP %d.", name+" health", response.StatusCode)
		}
	}
	return ""
}

func (c *claimCheck) checkServiceBErrors(ctx context.Context, hidden bool) string {
	cases := []struct {
		name, body string
		status     int
	}{{"unknown claim", `{"messageId":"does-not-exist","userName":"Unknown"}`, http.StatusNotFound}, {"malformed B payload", `{"messageId":`, http.StatusBadRequest}, {"B payload already has user ID", `{"messageId":"x","userId":"leaked"}`, http.StatusBadRequest}, {"B trailing JSON", `{"messageId":"x"} {}`, http.StatusBadRequest}, {"B non-object JSON", `[]`, http.StatusBadRequest}}
	for _, current := range cases {
		c.final.configure(http.StatusOK, "")
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, origin(c.serviceBPort)+"/messages", strings.NewReader(current.body))
		request.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			if hidden {
				return "failed"
			}
			return fmt.Sprintf("Check %q failed. Diagnosis: request service B: %v", current.name, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != current.status || len(c.final.snapshot()) != 0 {
			if hidden {
				return "failed"
			}
			return fmt.Sprintf("Check %q failed. Expected HTTP %d and no final call, got HTTP %d and %d final call(s).", current.name, current.status, response.StatusCode, len(c.final.snapshot()))
		}
	}
	return ""
}

func (c *claimCheck) clearClaims(ctx context.Context) error {
	iterator := c.redis.Scan(ctx, 0, "claim:*", 0).Iterator()
	for iterator.Next(ctx) {
		if err := c.redis.Del(ctx, iterator.Val()).Err(); err != nil {
			return err
		}
	}
	return iterator.Err()
}

func (c *claimCheck) ready() error {
	if c.initErr != nil {
		return c.initErr
	}
	if c.redis == nil || c.intermediary == nil || c.final == nil {
		return errors.New("claim check backends are not running")
	}
	return nil
}

func startRecordingBackend(port int) (*recordingBackend, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	backend := &recordingBackend{listener: listener, status: http.StatusOK}
	backend.server = &http.Server{Handler: http.HandlerFunc(backend.handle), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = backend.server.Serve(listener) }()
	return backend, nil
}

func (b *recordingBackend) configure(status int, response string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status, b.response, b.requests = status, response, nil
}

func (b *recordingBackend) handle(writer http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	b.mu.Lock()
	b.requests = append(b.requests, observedRequest{request.Method, request.URL.Path, append([]byte(nil), body...)})
	status, response := b.status, b.response
	b.mu.Unlock()
	if status == -1 {
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
		return
	}
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, response)
}

func (b *recordingBackend) snapshot() []observedRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]observedRequest(nil), b.requests...)
}

func startIntermediary(port int, client *redis.Client, serviceBURL string) (*intermediaryBackend, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	backend := &intermediaryBackend{listener: listener, redis: client, serviceBURL: serviceBURL, delivery: feedbackDelivery()}
	backend.server = &http.Server{Handler: http.HandlerFunc(backend.handle), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = backend.server.Serve(listener) }()
	return backend, nil
}

func (b *intermediaryBackend) configure(status int, response, expectedID string, delivery address) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status, b.response, b.expectedID, b.delivery = status, response, expectedID, delivery
	b.requests, b.messageIDs = nil, nil
}

func (b *intermediaryBackend) handle(writer http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	b.mu.Lock()
	b.requests = append(b.requests, observedRequest{request.Method, request.URL.Path, append([]byte(nil), body...)})
	status, response, expectedID, delivery := b.status, b.response, b.expectedID, b.delivery
	b.mu.Unlock()
	var message map[string]any
	if json.Unmarshal(body, &message) != nil {
		http.Error(writer, "invalid JSON", http.StatusUnprocessableEntity)
		return
	}
	if _, exists := message["userId"]; exists {
		http.Error(writer, "userId leaked to intermediary", http.StatusUnprocessableEntity)
		return
	}
	id, ok := message["messageId"].(string)
	if !ok || id == "" || id == expectedID {
		http.Error(writer, "invalid messageId", http.StatusUnprocessableEntity)
		return
	}
	value, err := b.redis.Get(request.Context(), "claim:"+id).Result()
	if err != nil || value != expectedID {
		http.Error(writer, "Redis claim missing or incorrect", http.StatusUnprocessableEntity)
		return
	}
	ttl, err := b.redis.TTL(request.Context(), "claim:"+id).Result()
	if err != nil || ttl <= 0 || ttl > claimTTL {
		http.Error(writer, "Redis claim TTL is invalid", http.StatusUnprocessableEntity)
		return
	}
	b.mu.Lock()
	b.messageIDs = append(b.messageIDs, id)
	b.mu.Unlock()
	if status == -1 {
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
		return
	}
	if status != 0 {
		writer.WriteHeader(status)
		_, _ = io.WriteString(writer, response)
		return
	}
	message["deliveryAddress"] = delivery
	forwarded, _ := json.Marshal(message)
	outgoing, _ := http.NewRequestWithContext(request.Context(), http.MethodPost, b.serviceBURL, bytes.NewReader(forwarded))
	outgoing.Header.Set("Content-Type", "application/json")
	result, err := (&http.Client{Timeout: 10 * time.Second}).Do(outgoing)
	if err != nil {
		http.Error(writer, "service B unreachable", http.StatusBadGateway)
		return
	}
	defer result.Body.Close()
	responseBody, _ := io.ReadAll(result.Body)
	writer.WriteHeader(result.StatusCode)
	_, _ = writer.Write(responseBody)
}

func (b *intermediaryBackend) snapshot() ([]observedRequest, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]observedRequest(nil), b.requests...), append([]string(nil), b.messageIDs...)
}

func (b *intermediaryBackend) stop() error { return stopServer(b.server, "intermediary backend") }
func (b *recordingBackend) stop() error    { return stopServer(b.server, "final backend") }

func stopServer(server *http.Server, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("stop %s: %w", name, err)
	}
	return nil
}

func origin(port int) string {
	return (&url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", port)}).String()
}

func failure(current check, diagnosis string) checkResult {
	return checkResult{feedback: fmt.Sprintf("Check %q failed. Request: POST service A /messages body=%q. Diagnosis: %s", current.name, current.body, diagnosis)}
}
