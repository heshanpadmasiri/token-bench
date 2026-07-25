// Package contentbasedrouting implements the Content-Based Router task.
package contentbasedrouting

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"text/template"
	"time"

	"token-bench/task/internal/jsoncompare"
)

const requiredPorts = 3

//go:embed prompt.md.tmpl
var promptTemplate string

type contentBasedRouting struct {
	applicationPort int
	ordersPort      int
	invoicesPort    int
	orders          *backend
	invoices        *backend
	initErr         error
}

type backend struct {
	name       string
	statusCode int
	response   string
	server     *http.Server
	listener   net.Listener
	mu         sync.Mutex
	requests   []observedRequest
}

type observedRequest struct {
	method string
	host   string
	path   string
	query  string
	header http.Header
	body   []byte
}

type check struct {
	name            string
	body            string
	query           string
	header          http.Header
	expectedBackend *backend
	expectedStatus  int
}

type checkResult struct {
	passed   bool
	feedback string
}

// PromptTemplate returns the unrendered task requirements.
func PromptTemplate() string {
	return promptTemplate
}

// New initializes the private Content-Based Router implementation.
func New(ports []int) *contentBasedRouting {
	handle := &contentBasedRouting{}
	if len(ports) != requiredPorts {
		handle.initErr = fmt.Errorf("content-based router requires %d ports, got %d", requiredPorts, len(ports))
		return handle
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			handle.initErr = fmt.Errorf("invalid allocated port %d", port)
			return handle
		}
	}
	handle.applicationPort = ports[0]
	handle.ordersPort = ports[1]
	handle.invoicesPort = ports[2]
	return handle
}

func (c *contentBasedRouting) Setup() (func() error, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	if c.orders != nil || c.invoices != nil {
		return nil, errors.New("content-based router is already set up")
	}
	orders, err := startBackend(c.ordersPort, "orders", http.StatusCreated, "{\n  \"backend\": \"orders\"\n}")
	if err != nil {
		return nil, fmt.Errorf("start orders backend: %w", err)
	}
	invoices, err := startBackend(c.invoicesPort, "invoices", http.StatusAccepted, "{ \"backend\" : \"invoices\" }")
	if err != nil {
		_ = orders.stop()
		return nil, fmt.Errorf("start invoices backend: %w", err)
	}
	c.orders = orders
	c.invoices = invoices
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() {
			cleanupErr = errors.Join(c.orders.stop(), c.invoices.stop())
			c.orders = nil
			c.invoices = nil
		})
		return cleanupErr
	}, nil
}

func (c *contentBasedRouting) Prompt() (string, error) {
	if c.initErr != nil {
		return "", c.initErr
	}
	parsed, err := template.New("content-based-router").Parse(promptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse prompt template: %w", err)
	}
	data := struct {
		OrdersURL   string
		InvoicesURL string
	}{origin(c.ordersPort), origin(c.invoicesPort)}
	var prompt bytes.Buffer
	if err := parsed.Execute(&prompt, data); err != nil {
		return "", fmt.Errorf("render prompt template: %w", err)
	}
	return prompt.String(), nil
}

func (c *contentBasedRouting) Feedback() (string, error) {
	if err := c.ready(); err != nil {
		return "", err
	}
	if feedback := checkHealth(c.applicationPort); feedback != "" {
		return feedback, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := c.runChecks(ctx, []check{
		{
			name:            "order routes to orders",
			body:            "{\n  \"messageType\": \"order\",\n  \"id\": \"order-1042\",\n  \"payload\": {\"amount\": 42}\n}",
			query:           "source=feedback&case=order",
			header:          http.Header{"Content-Type": {"application/json; charset=utf-8"}, "X-Correlation-Id": {"order-1042"}, "X-Token-Bench": {"preserve-me"}, "Connection": {"X-Hop-By-Hop"}, "X-Hop-By-Hop": {"remove-me"}},
			expectedBackend: c.orders,
			expectedStatus:  http.StatusCreated,
		},
		{
			name:            "invoice routes to invoices",
			body:            `{"payload":{"messageType":"order"},"messageType":"invoice","id":73}`,
			query:           "source=feedback&case=invoice",
			header:          http.Header{"Content-Type": {"application/json"}, "X-Correlation-Id": {"invoice-73"}},
			expectedBackend: c.invoices,
			expectedStatus:  http.StatusAccepted,
		},
		{name: "unknown type returns 400", body: `{"messageType":"shipment"}`, expectedStatus: http.StatusBadRequest},
		{name: "missing type returns 400", body: `{"id":10}`, expectedStatus: http.StatusBadRequest},
		{name: "malformed JSON returns 400", body: `{"messageType":"order"`, expectedStatus: http.StatusBadRequest},
		{name: "trailing JSON returns 400", body: `{"messageType":"order"} {}`, expectedStatus: http.StatusBadRequest},
		{name: "non-object JSON returns 400", body: `[]`, expectedStatus: http.StatusBadRequest},
	})
	return result.feedback, nil
}

func (c *contentBasedRouting) Validation() (bool, error) {
	if err := c.ready(); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := c.runChecks(ctx, []check{
		{
			name:            "hidden order",
			body:            `{"id":"hidden-order","messageType":"order","items":[1,2]}`,
			query:           "suite=final&case=order",
			header:          http.Header{"Content-Type": {"application/json"}, "X-Correlation-Id": {"hidden-order"}},
			expectedBackend: c.orders,
			expectedStatus:  http.StatusCreated,
		},
		{
			name:            "hidden invoice",
			body:            "{ \"id\": \"hidden-invoice\", \"messageType\" : \"invoice\" }",
			query:           "suite=final&case=invoice",
			header:          http.Header{"Content-Type": {"application/json; charset=utf-8"}, "X-Token-Bench": {"hidden-header"}},
			expectedBackend: c.invoices,
			expectedStatus:  http.StatusAccepted,
		},
		{name: "hidden non-string type", body: `{"messageType":["order"]}`, expectedStatus: http.StatusBadRequest},
		{name: "hidden unknown type", body: `{"messageType":"credit-note"}`, expectedStatus: http.StatusBadRequest},
	})
	return result.passed, nil
}

func (c *contentBasedRouting) ready() error {
	if c.initErr != nil {
		return c.initErr
	}
	if c.orders == nil || c.invoices == nil {
		return errors.New("content-based router backends are not running")
	}
	return nil
}

func (c *contentBasedRouting) runChecks(ctx context.Context, checks []check) checkResult {
	for _, current := range checks {
		result := c.runCheck(ctx, current)
		if !result.passed {
			return result
		}
	}
	return checkResult{passed: true}
}

func (c *contentBasedRouting) runCheck(ctx context.Context, current check) checkResult {
	c.orders.reset()
	c.invoices.reset()
	endpoint := origin(c.applicationPort) + "/messages"
	if current.query != "" {
		endpoint += "?" + current.query
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(current.body))
	if err != nil {
		return failure(current, fmt.Sprintf("construct request: %v", err))
	}
	request.Header = current.header.Clone()
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return failure(current, fmt.Sprintf("request router: %v", err))
	}
	responseBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		return failure(current, fmt.Sprintf("read response: %v", readErr))
	}
	if response.StatusCode != current.expectedStatus {
		return failure(current, fmt.Sprintf("expected HTTP %d, got HTTP %d with body %q", current.expectedStatus, response.StatusCode, responseBody))
	}

	orders := c.orders.snapshot()
	invoices := c.invoices.snapshot()
	if current.expectedBackend == nil {
		if len(orders) != 0 || len(invoices) != 0 {
			return failure(current, fmt.Sprintf("rejected message contacted backends: orders=%d invoices=%d", len(orders), len(invoices)))
		}
		return checkResult{passed: true}
	}

	expected, unexpected := orders, invoices
	unexpectedName := "invoices"
	if current.expectedBackend == c.invoices {
		expected, unexpected = invoices, orders
		unexpectedName = "orders"
	}
	if len(expected) != 1 {
		return failure(current, fmt.Sprintf("expected one request at %s backend, got %d", current.expectedBackend.name, len(expected)))
	}
	if len(unexpected) != 0 {
		return failure(current, fmt.Sprintf("unexpectedly contacted %s backend", unexpectedName))
	}
	observed := expected[0]
	expectedHost := strings.TrimPrefix(origin(c.ordersPort), "http://")
	if current.expectedBackend == c.invoices {
		expectedHost = strings.TrimPrefix(origin(c.invoicesPort), "http://")
	}
	if observed.method != http.MethodPost || observed.host != expectedHost || observed.path != "/messages" || observed.query != current.query {
		return failure(current, fmt.Sprintf("backend received method=%q host=%q path=%q query=%q", observed.method, observed.host, observed.path, observed.query))
	}
	if string(observed.body) != current.body {
		return failure(current, fmt.Sprintf("backend body changed: expected %q, got %q", current.body, observed.body))
	}
	for _, name := range []string{"Content-Type", "X-Correlation-Id", "X-Token-Bench"} {
		if !sameStrings(observed.header.Values(name), current.header.Values(name)) {
			return failure(current, fmt.Sprintf("backend header %s changed: expected %q, got %q", name, current.header.Values(name), observed.header.Values(name)))
		}
	}
	for _, name := range []string{"Connection", "X-Hop-By-Hop"} {
		if len(observed.header.Values(name)) != 0 {
			return failure(current, fmt.Sprintf("backend received hop-by-hop header %s=%q", name, observed.header.Values(name)))
		}
	}
	expectedResponse := []byte(fmt.Sprintf(`{"backend":%q}`, current.expectedBackend.name))
	equal, compareErr := jsoncompare.Equal(expectedResponse, responseBody)
	if compareErr != nil || !equal {
		return failure(current, fmt.Sprintf("backend JSON response was not relayed: expected %q, got %q (comparison error: %v)", expectedResponse, responseBody, compareErr))
	}
	if !jsoncompare.IsJSONContentType(response.Header.Get("Content-Type")) || response.Header.Get("X-Token-Bench-Backend") != current.expectedBackend.name {
		return failure(current, fmt.Sprintf("backend response headers were not relayed: Content-Type=%q X-Token-Bench-Backend=%q", response.Header.Get("Content-Type"), response.Header.Get("X-Token-Bench-Backend")))
	}
	if response.Header.Get("Connection") != "" || response.Header.Get("X-Backend-Hop") != "" {
		return failure(current, fmt.Sprintf("hop-by-hop response headers were relayed: Connection=%q X-Backend-Hop=%q", response.Header.Get("Connection"), response.Header.Get("X-Backend-Hop")))
	}
	return checkResult{passed: true}
}

func startBackend(port int, name string, statusCode int, response string) (*backend, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	backend := &backend{name: name, statusCode: statusCode, response: response, listener: listener}
	backend.server = &http.Server{Handler: http.HandlerFunc(backend.handle), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = backend.server.Serve(listener) }()
	return backend, nil
}

func (b *backend) handle(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	b.requests = append(b.requests, observedRequest{
		method: request.Method,
		host:   request.Host,
		path:   request.URL.Path,
		query:  request.URL.RawQuery,
		header: request.Header.Clone(),
		body:   append([]byte(nil), body...),
	})
	b.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("X-Token-Bench-Backend", b.name)
	writer.Header().Set("Connection", "X-Backend-Hop")
	writer.Header().Set("X-Backend-Hop", "remove-me")
	writer.WriteHeader(b.statusCode)
	_, _ = io.WriteString(writer, b.response)
}

func (b *backend) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.requests = nil
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
		return fmt.Errorf("stop %s backend: %w", b.name, err)
	}
	return nil
}

func checkHealth(port int) string {
	response, err := (&http.Client{Timeout: 5 * time.Second}).Get(origin(port) + "/health")
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

func origin(port int) string {
	return (&url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", port)}).String()
}

func failure(current check, diagnosis string) checkResult {
	query := ""
	if current.query != "" {
		query = "?" + current.query
	}
	return checkResult{feedback: fmt.Sprintf("Check %q failed. Request: POST /messages%s body=%q. Diagnosis: %s", current.name, query, current.body, diagnosis)}
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
