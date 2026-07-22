// Package contentenricher implements the Content Enricher task with a
// Dockerized PostgreSQL database and a benchmark-owned HTTP backend.
package contentenricher

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
	"reflect"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	requiredPorts = 3
	postgresImage = "postgres:16-alpine"
	databaseUser  = "tokenbench"
	databasePass  = "tokenbench"
	databaseName  = "tokenbench"
)

//go:embed prompt.md.tmpl
var promptTemplate string

type contentEnricher struct {
	applicationPort int
	databasePort    int
	backendPort     int
	containerName   string
	database        *pgx.Conn
	backend         *backend
	initErr         error
}

type address struct {
	Street     string `json:"street"`
	City       string `json:"city"`
	State      string `json:"state"`
	PostalCode string `json:"postalCode"`
	Country    string `json:"country"`
}

type userAddress struct {
	UserID  string
	Address address
}

type backend struct {
	server   *http.Server
	listener net.Listener
	mu       sync.Mutex
	status   int
	response string
	requests []observedRequest
}

type observedRequest struct {
	method string
	path   string
	body   []byte
}

type check struct {
	name              string
	body              string
	expectedStatus    int
	expectedResponse  string
	expectedAddress   *address
	backendStatus     int
	backendResponse   string
	expectBackendCall bool
}

type checkResult struct {
	passed   bool
	feedback string
}

// PromptTemplate returns the unrendered task requirements.
func PromptTemplate() string {
	return promptTemplate
}

// New initializes the private Content Enricher implementation.
func New(ports []int) *contentEnricher {
	handle := &contentEnricher{}
	if len(ports) != requiredPorts {
		handle.initErr = fmt.Errorf("content enricher requires %d ports, got %d", requiredPorts, len(ports))
		return handle
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			handle.initErr = fmt.Errorf("invalid allocated port %d", port)
			return handle
		}
	}
	handle.applicationPort = ports[0]
	handle.databasePort = ports[1]
	handle.backendPort = ports[2]
	return handle
}

func (c *contentEnricher) Setup() (func() error, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	if c.containerName != "" || c.backend != nil {
		return nil, errors.New("content enricher is already set up")
	}
	c.containerName = fmt.Sprintf("token-bench-postgres-%d-%d", c.databasePort, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm", "--name", c.containerName,
		"--env", "POSTGRES_USER="+databaseUser,
		"--env", "POSTGRES_PASSWORD="+databasePass,
		"--env", "POSTGRES_DB="+databaseName,
		"--publish", fmt.Sprintf("127.0.0.1:%d:5432", c.databasePort), postgresImage).CombinedOutput()
	if err != nil {
		c.containerName = ""
		return nil, fmt.Errorf("start PostgreSQL container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := c.connectAndSeed(ctx); err != nil {
		_ = c.cleanup()
		return nil, err
	}
	c.backend, err = startBackend(c.backendPort)
	if err != nil {
		_ = c.cleanup()
		return nil, fmt.Errorf("start downstream backend: %w", err)
	}
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() { cleanupErr = c.cleanup() })
		return cleanupErr
	}, nil
}

func (c *contentEnricher) connectAndSeed(ctx context.Context) error {
	var err error
	for {
		c.database, err = pgx.Connect(ctx, c.databaseURL())
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for PostgreSQL: %w", errors.Join(err, ctx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
	_, err = c.database.Exec(ctx, `
CREATE TABLE user_addresses (
    user_id TEXT PRIMARY KEY,
    street TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postal_code TEXT NOT NULL,
    country TEXT NOT NULL
)`)
	if err != nil {
		return fmt.Errorf("create address table: %w", err)
	}
	for _, row := range allAddresses() {
		_, err := c.database.Exec(ctx, `INSERT INTO user_addresses (user_id, street, city, state, postal_code, country) VALUES ($1, $2, $3, $4, $5, $6)`,
			row.UserID, row.Address.Street, row.Address.City, row.Address.State, row.Address.PostalCode, row.Address.Country)
		if err != nil {
			return fmt.Errorf("seed address for %s: %w", row.UserID, err)
		}
	}
	return nil
}

func (c *contentEnricher) cleanup() error {
	var failures []error
	if c.backend != nil {
		failures = append(failures, c.backend.stop())
	}
	if c.database != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.database.Close(ctx); err != nil {
			failures = append(failures, fmt.Errorf("close PostgreSQL connection: %w", err))
		}
		cancel()
	}
	if c.containerName != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		output, err := exec.CommandContext(ctx, "docker", "rm", "--force", c.containerName).CombinedOutput()
		cancel()
		if err != nil && !strings.Contains(string(output), "No such container") {
			failures = append(failures, fmt.Errorf("remove PostgreSQL container: %w: %s", err, strings.TrimSpace(string(output))))
		}
	}
	c.backend, c.database, c.containerName = nil, nil, ""
	return errors.Join(failures...)
}

func (c *contentEnricher) Prompt() (string, error) {
	if c.initErr != nil {
		return "", c.initErr
	}
	parsed, err := template.New("content-enricher").Parse(promptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse prompt template: %w", err)
	}
	data := struct {
		DatabaseHost  string
		DatabasePort  int
		DatabaseName  string
		DatabaseUser  string
		DatabasePass  string
		DatabaseTable string
		DownstreamURL string
	}{
		DatabaseHost:  "127.0.0.1",
		DatabasePort:  c.databasePort,
		DatabaseName:  databaseName,
		DatabaseUser:  databaseUser,
		DatabasePass:  databasePass,
		DatabaseTable: "user_addresses",
		DownstreamURL: origin(c.backendPort) + "/enriched-messages",
	}
	var prompt bytes.Buffer
	if err := parsed.Execute(&prompt, data); err != nil {
		return "", fmt.Errorf("render prompt template: %w", err)
	}
	return prompt.String(), nil
}

func feedbackAddresses() []userAddress {
	return []userAddress{
		{UserID: "user-1042", Address: address{Street: "12 Example Street", City: "London", State: "Greater London", PostalCode: "SW1A 1AA", Country: "GB"}},
		{UserID: "user-73", Address: address{Street: "73 Market Avenue", City: "San Francisco", State: "CA", PostalCode: "94105", Country: "US"}},
	}
}

func validationAddresses() []userAddress {
	return []userAddress{
		{UserID: "hidden-user-19", Address: address{Street: "8 Harbour Road", City: "Sydney", State: "NSW", PostalCode: "2000", Country: "AU"}},
		{UserID: "hidden-user-88", Address: address{Street: "4-2 Sakura Lane", City: "Kyoto", State: "Kyoto", PostalCode: "600-8216", Country: "JP"}},
	}
}

func allAddresses() []userAddress {
	return append(feedbackAddresses(), validationAddresses()...)
}

func (c *contentEnricher) Feedback() (string, error) {
	if err := c.ready(); err != nil {
		return "", err
	}
	rows := feedbackAddresses()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := c.runChecks(ctx, []check{
		{name: "enriches a known user", body: `{"messageId":"message-1042","user":{"id":"user-1042","name":"Ada Lovelace","dateOfBirth":"1815-12-10"},"payload":{"orderId":"order-73"}}`, expectedStatus: http.StatusAccepted, expectedAddress: &rows[0].Address, backendStatus: http.StatusOK, backendResponse: `{"stored":true}`, expectBackendCall: true},
		{name: "successful downstream status is normalized", body: `{"messageId":73,"user":{"id":"user-73","name":"Grace Hopper"},"tags":["public",42]}`, expectedStatus: http.StatusAccepted, expectedAddress: &rows[1].Address, backendStatus: http.StatusNoContent, expectBackendCall: true},
		{name: "downstream error is relayed", body: `{"user":{"id":"user-1042"},"operation":"reject"}`, expectedStatus: http.StatusUnprocessableEntity, expectedResponse: `{"error":"rejected"}`, expectedAddress: &rows[0].Address, backendStatus: http.StatusUnprocessableEntity, backendResponse: `{"error":"rejected"}`, expectBackendCall: true},
		{name: "unknown user returns 404", body: `{"user":{"id":"missing-user"}}`, expectedStatus: http.StatusNotFound, backendStatus: http.StatusOK},
		{name: "existing top-level address returns 400", body: `{"user":{"id":"user-1042"},"address":{"city":"supplied by caller"}}`, expectedStatus: http.StatusBadRequest, backendStatus: http.StatusOK},
		{name: "existing nested address returns 400", body: `{"user":{"id":"user-1042","address":{"city":"supplied by caller"}}}`, expectedStatus: http.StatusBadRequest, backendStatus: http.StatusOK},
		{name: "malformed JSON returns 400", body: `{"user":{"id":"user-1042"}`, expectedStatus: http.StatusBadRequest, backendStatus: http.StatusOK},
		{name: "missing user id returns 400", body: `{"user":{"name":"No Identifier"}}`, expectedStatus: http.StatusBadRequest, backendStatus: http.StatusOK},
	})
	return result.feedback, nil
}

func (c *contentEnricher) Validation() (bool, error) {
	if err := c.ready(); err != nil {
		return false, err
	}
	rows := validationAddresses()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := c.runChecks(ctx, []check{
		{name: "hidden enrichment", body: `{"metadata":{"attempt":2,"active":true},"user":{"name":"Hidden User","id":"hidden-user-19","dateOfBirth":"2001-01-02"},"messageId":"hidden-19"}`, expectedStatus: http.StatusAccepted, expectedAddress: &rows[0].Address, backendStatus: http.StatusCreated, backendResponse: `created`, expectBackendCall: true},
		{name: "hidden nested fields are preserved", body: `{"user":{"id":"hidden-user-88","preferences":{"languages":["ja","en"]}},"payload":{"amount":19.95,"nullable":null}}`, expectedStatus: http.StatusAccepted, expectedAddress: &rows[1].Address, backendStatus: http.StatusOK, backendResponse: `ignored`, expectBackendCall: true},
		{name: "hidden non-string id", body: `{"user":{"id":88}}`, expectedStatus: http.StatusBadRequest, backendStatus: http.StatusOK},
		{name: "hidden empty id", body: `{"user":{"id":""}}`, expectedStatus: http.StatusBadRequest, backendStatus: http.StatusOK},
		{name: "hidden existing address", body: `{"user":{"id":"hidden-user-19"},"address":null}`, expectedStatus: http.StatusBadRequest, backendStatus: http.StatusOK},
	})
	return result.passed, nil
}

func (c *contentEnricher) runChecks(ctx context.Context, checks []check) checkResult {
	for _, current := range checks {
		result := c.runCheck(ctx, current)
		if !result.passed {
			return result
		}
	}
	return checkResult{passed: true}
}

func (c *contentEnricher) runCheck(ctx context.Context, current check) checkResult {
	c.backend.configure(current.backendStatus, current.backendResponse)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, origin(c.applicationPort)+"/messages", strings.NewReader(current.body))
	if err != nil {
		return failure(current, fmt.Sprintf("construct request: %v", err))
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return failure(current, fmt.Sprintf("request application: %v", err))
	}
	responseBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		return failure(current, fmt.Sprintf("read response: %v", readErr))
	}
	if response.StatusCode != current.expectedStatus {
		return failure(current, fmt.Sprintf("expected HTTP %d, got HTTP %d with body %q", current.expectedStatus, response.StatusCode, responseBody))
	}
	if string(responseBody) != current.expectedResponse {
		return failure(current, fmt.Sprintf("expected response body %q, got %q", current.expectedResponse, responseBody))
	}
	requests := c.backend.snapshot()
	if !current.expectBackendCall {
		if len(requests) != 0 {
			return failure(current, fmt.Sprintf("invalid or unenrichable input contacted downstream %d time(s)", len(requests)))
		}
		return checkResult{passed: true}
	}
	if len(requests) != 1 {
		return failure(current, fmt.Sprintf("expected one downstream request, got %d", len(requests)))
	}
	observed := requests[0]
	if observed.method != http.MethodPost || observed.path != "/enriched-messages" {
		return failure(current, fmt.Sprintf("downstream received method=%q path=%q", observed.method, observed.path))
	}
	var expected, actual map[string]any
	if err := json.Unmarshal([]byte(current.body), &expected); err != nil {
		return failure(current, fmt.Sprintf("decode expected request: %v", err))
	}
	expected["address"] = current.expectedAddress
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return failure(current, fmt.Sprintf("encode expected enriched request: %v", err))
	}
	if err := json.Unmarshal(expectedJSON, &expected); err != nil {
		return failure(current, fmt.Sprintf("normalize expected enriched request: %v", err))
	}
	if err := json.Unmarshal(observed.body, &actual); err != nil {
		return failure(current, fmt.Sprintf("downstream body is not valid JSON: %v", err))
	}
	if !reflect.DeepEqual(actual, expected) {
		return failure(current, fmt.Sprintf("downstream body was not the preserved, enriched message: expected %s, got %s", expectedJSON, observed.body))
	}
	return checkResult{passed: true}
}

func (c *contentEnricher) ready() error {
	if c.initErr != nil {
		return c.initErr
	}
	if c.database == nil || c.backend == nil {
		return errors.New("content enricher backends are not running")
	}
	return nil
}

func startBackend(port int) (*backend, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	current := &backend{listener: listener, status: http.StatusOK}
	current.server = &http.Server{Handler: http.HandlerFunc(current.handle), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = current.server.Serve(listener) }()
	return current, nil
}

func (b *backend) configure(status int, response string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = status
	b.response = response
	b.requests = nil
}

func (b *backend) handle(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	b.requests = append(b.requests, observedRequest{method: request.Method, path: request.URL.Path, body: append([]byte(nil), body...)})
	status, response := b.status, b.response
	b.mu.Unlock()
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, response)
}

func (b *backend) snapshot() []observedRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]observedRequest(nil), b.requests...)
}

func (b *backend) stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("stop downstream backend: %w", err)
	}
	return nil
}

func (c *contentEnricher) databaseURL() string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", databaseUser, databasePass, c.databasePort, databaseName)
}

func origin(port int) string {
	return (&url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", port)}).String()
}

func failure(current check, diagnosis string) checkResult {
	return checkResult{feedback: fmt.Sprintf("Check %q failed. Request: POST /messages body=%q. Diagnosis: %s", current.name, current.body, diagnosis)}
}
