// Package scattergather implements the Scatter-Gather task.
package scattergather

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
	"strings"
	"sync"
	"text/template"
	"time"
)

const (
	requiredPorts  = 4
	backendTimeout = 3 * time.Second
)

//go:embed prompt.md.tmpl
var promptTemplate string

type scatterGather struct {
	applicationPort int
	backendPorts    []int
	backends        []*backend
	initErr         error
}

type backend struct {
	name     string
	server   *http.Server
	listener net.Listener
	mu       sync.Mutex
	status   int
	response string
	gate     *barrier
	requests []observedRequest
}

type observedRequest struct {
	method string
	path   string
	query  string
	header http.Header
	body   []byte
}

type barrier struct {
	mu      sync.Mutex
	arrived int
	release chan struct{}
}

type check struct {
	name             string
	body             string
	query            string
	header           http.Header
	backendStatuses  []int
	backendResponses []string
	expectedStatus   int
	expectFanout     bool
}

type checkResult struct {
	passed   bool
	feedback string
}

// PromptTemplate returns the unrendered task requirements.
func PromptTemplate() string {
	return promptTemplate
}

// New initializes the private Scatter-Gather implementation.
func New(ports []int) *scatterGather {
	handle := &scatterGather{}
	if len(ports) != requiredPorts {
		handle.initErr = fmt.Errorf("scatter-gather requires %d ports, got %d", requiredPorts, len(ports))
		return handle
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			handle.initErr = fmt.Errorf("invalid allocated port %d", port)
			return handle
		}
	}
	handle.applicationPort = ports[0]
	handle.backendPorts = append([]int(nil), ports[1:]...)
	return handle
}

func (s *scatterGather) Setup() (func() error, error) {
	if s.initErr != nil {
		return nil, s.initErr
	}
	if len(s.backends) != 0 {
		return nil, errors.New("scatter-gather is already set up")
	}
	for index, name := range []string{"alpha", "beta", "gamma"} {
		current, err := startBackend(s.backendPorts[index], name)
		if err != nil {
			for _, started := range s.backends {
				_ = started.stop()
			}
			s.backends = nil
			return nil, fmt.Errorf("start %s backend: %w", name, err)
		}
		s.backends = append(s.backends, current)
	}
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() {
			var failures []error
			for _, current := range s.backends {
				failures = append(failures, current.stop())
			}
			cleanupErr = errors.Join(failures...)
			s.backends = nil
		})
		return cleanupErr
	}, nil
}

func (s *scatterGather) Prompt() (string, error) {
	if s.initErr != nil {
		return "", s.initErr
	}
	parsed, err := template.New("scatter-gather").Parse(promptTemplate)
	if err != nil {
		return "", fmt.Errorf("parse prompt template: %w", err)
	}
	data := struct {
		AlphaURL string
		BetaURL  string
		GammaURL string
	}{origin(s.backendPorts[0]), origin(s.backendPorts[1]), origin(s.backendPorts[2])}
	var prompt bytes.Buffer
	if err := parsed.Execute(&prompt, data); err != nil {
		return "", fmt.Errorf("render prompt template: %w", err)
	}
	return prompt.String(), nil
}

func (s *scatterGather) Feedback() (string, error) {
	if err := s.ready(); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := s.runChecks(ctx, []check{
		{
			name:            "gathers three quotes concurrently",
			body:            `{"product":"widget","quantity":2}`,
			query:           "region=us&case=feedback",
			header:          http.Header{"Content-Type": {"application/json"}, "X-Correlation-Id": {"quote-1042"}, "X-Token-Bench": {"preserve-me"}},
			backendStatuses: []int{http.StatusOK, http.StatusOK, http.StatusOK},
			backendResponses: []string{
				`{"supplier":"alpha","amount":101.25,"currency":"USD"}`,
				`{"supplier":"beta","amount":99.5,"currency":"USD"}`,
				`{"supplier":"gamma","amount":103,"currency":"USD"}`,
			},
			expectedStatus: http.StatusOK,
			expectFanout:   true,
		},
		{
			name:             "backend failure returns 502",
			body:             `{"product":"cable","quantity":5}`,
			query:            "case=backend-error",
			header:           http.Header{"Content-Type": {"application/json; charset=utf-8"}, "X-Correlation-Id": {"quote-error"}},
			backendStatuses:  []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusOK},
			backendResponses: []string{`{"supplier":"alpha","amount":20}`, `{"error":"unavailable"}`, `{"supplier":"gamma","amount":22}`},
			expectedStatus:   http.StatusBadGateway,
			expectFanout:     true,
		},
		{
			name:             "malformed JSON returns 400",
			body:             `{"product":"widget"`,
			header:           http.Header{"Content-Type": {"application/json"}},
			backendStatuses:  []int{http.StatusOK, http.StatusOK, http.StatusOK},
			backendResponses: []string{`{}`, `{}`, `{}`},
			expectedStatus:   http.StatusBadRequest,
		},
		{
			name:             "schema violation returns 400",
			body:             `{"product":"widget","quantity":"2"}`,
			header:           http.Header{"Content-Type": {"application/json"}},
			backendStatuses:  []int{http.StatusOK, http.StatusOK, http.StatusOK},
			backendResponses: []string{`{}`, `{}`, `{}`},
			expectedStatus:   http.StatusBadRequest,
		},
	})
	return result.feedback, nil
}

func (s *scatterGather) Validation() (bool, error) {
	if err := s.ready(); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := s.runChecks(ctx, []check{
		{
			name:            "hidden quote aggregation",
			body:            "{\n  \"quantity\": 7,\n  \"product\": \"hidden-adapter\"\n}",
			query:           "suite=final&region=eu",
			header:          http.Header{"Content-Type": {"application/json; charset=utf-8"}, "X-Correlation-Id": {"hidden-quote"}, "X-Token-Bench": {"hidden-header"}},
			backendStatuses: []int{http.StatusOK, http.StatusOK, http.StatusOK},
			backendResponses: []string{
				`{"supplier":"alpha","amount":71.75,"currency":"EUR","available":true}`,
				`{"supplier":"beta","amount":69.25,"currency":"EUR","available":true}`,
				`{"supplier":"gamma","amount":75,"currency":"EUR","available":false}`,
			},
			expectedStatus: http.StatusOK,
			expectFanout:   true,
		},
		{
			name:             "hidden malformed JSON",
			body:             `not-json`,
			header:           http.Header{"Content-Type": {"application/json"}},
			backendStatuses:  []int{http.StatusOK, http.StatusOK, http.StatusOK},
			backendResponses: []string{`{}`, `{}`, `{}`},
			expectedStatus:   http.StatusBadRequest,
		},
		{
			name:             "hidden missing required field",
			body:             `{"quantity":3}`,
			header:           http.Header{"Content-Type": {"application/json"}},
			backendStatuses:  []int{http.StatusOK, http.StatusOK, http.StatusOK},
			backendResponses: []string{`{}`, `{}`, `{}`},
			expectedStatus:   http.StatusBadRequest,
		},
	})
	return result.passed, nil
}

func (s *scatterGather) ready() error {
	if s.initErr != nil {
		return s.initErr
	}
	if len(s.backends) != 3 {
		return errors.New("scatter-gather backends are not running")
	}
	return nil
}

func (s *scatterGather) runChecks(ctx context.Context, checks []check) checkResult {
	for _, current := range checks {
		result := s.runCheck(ctx, current)
		if !result.passed {
			return result
		}
	}
	return checkResult{passed: true}
}

func (s *scatterGather) runCheck(ctx context.Context, current check) checkResult {
	gate := &barrier{release: make(chan struct{})}
	for index, currentBackend := range s.backends {
		currentBackend.configure(current.backendStatuses[index], current.backendResponses[index], gate)
	}
	endpoint := origin(s.applicationPort) + "/quotes"
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

	for _, currentBackend := range s.backends {
		requests := currentBackend.snapshot()
		if !current.expectFanout {
			if len(requests) != 0 {
				return failure(current, fmt.Sprintf("invalid request contacted %s backend", currentBackend.name))
			}
			continue
		}
		if len(requests) != 1 {
			return failure(current, fmt.Sprintf("expected one request at %s backend, got %d", currentBackend.name, len(requests)))
		}
		observed := requests[0]
		if observed.method != http.MethodPost || observed.path != "/quotes" || observed.query != current.query {
			return failure(current, fmt.Sprintf("%s backend received method=%q path=%q query=%q", currentBackend.name, observed.method, observed.path, observed.query))
		}
		if string(observed.body) != current.body {
			return failure(current, fmt.Sprintf("%s backend body changed: expected %q, got %q", currentBackend.name, current.body, observed.body))
		}
		for _, name := range []string{"Content-Type", "X-Correlation-Id", "X-Token-Bench"} {
			if !sameStrings(observed.header.Values(name), current.header.Values(name)) {
				return failure(current, fmt.Sprintf("%s backend header %s changed: expected %q, got %q", currentBackend.name, name, current.header.Values(name), observed.header.Values(name)))
			}
		}
	}
	if current.expectedStatus == http.StatusOK {
		if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
			return failure(current, fmt.Sprintf("expected application/json response, got Content-Type %q", response.Header.Get("Content-Type")))
		}
		if err := validateQuotes(responseBody, current.backendResponses); err != nil {
			return failure(current, err.Error())
		}
	}
	return checkResult{passed: true}
}

func validateQuotes(body []byte, expected []string) error {
	var envelope struct {
		Quotes []json.RawMessage `json:"quotes"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("response is not valid JSON: %v", err)
	}
	if len(envelope.Quotes) != len(expected) {
		return fmt.Errorf("expected %d gathered quotes, got %d in %q", len(expected), len(envelope.Quotes), body)
	}
	remaining := make(map[string]int, len(expected))
	for _, quote := range expected {
		canonical, err := canonicalJSON([]byte(quote))
		if err != nil {
			return err
		}
		remaining[canonical]++
	}
	for _, quote := range envelope.Quotes {
		canonical, err := canonicalJSON(quote)
		if err != nil {
			return fmt.Errorf("gathered quote is invalid JSON: %v", err)
		}
		if remaining[canonical] == 0 {
			return fmt.Errorf("response contains unexpected quote %s", quote)
		}
		remaining[canonical]--
	}
	return nil
}

func canonicalJSON(value []byte) (string, error) {
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(decoded)
	return string(canonical), err
}

func startBackend(port int, name string) (*backend, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	amounts := map[string]float64{"alpha": 101.25, "beta": 99.5, "gamma": 103}
	response, err := json.Marshal(map[string]any{
		"supplier": name,
		"amount":   amounts[name],
		"currency": "USD",
	})
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("create %s default response: %w", name, err)
	}
	current := &backend{
		name:     name,
		listener: listener,
		status:   http.StatusOK,
		response: string(response),
	}
	current.server = &http.Server{Handler: http.HandlerFunc(current.handle), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = current.server.Serve(listener) }()
	return current, nil
}

func (b *backend) configure(status int, response string, gate *barrier) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = status
	b.response = response
	b.gate = gate
	b.requests = nil
}

func (b *backend) handle(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	b.requests = append(b.requests, observedRequest{request.Method, request.URL.Path, request.URL.RawQuery, request.Header.Clone(), append([]byte(nil), body...)})
	status, response, gate := b.status, b.response, b.gate
	b.mu.Unlock()
	if gate != nil {
		select {
		case <-gate.arrive():
		case <-request.Context().Done():
			return
		case <-time.After(backendTimeout):
			http.Error(writer, "requests were not scattered concurrently", http.StatusGatewayTimeout)
			return
		}
	}
	writer.Header().Set("Content-Type", "application/json")
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
		return fmt.Errorf("stop %s backend: %w", b.name, err)
	}
	return nil
}

func (b *barrier) arrive() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.arrived++
	if b.arrived == 3 {
		close(b.release)
	}
	return b.release
}

func origin(port int) string {
	return (&url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", port)}).String()
}

func failure(current check, diagnosis string) checkResult {
	query := ""
	if current.query != "" {
		query = "?" + current.query
	}
	return checkResult{feedback: fmt.Sprintf("Check %q failed. Request: POST /quotes%s body=%q. Diagnosis: %s", current.name, query, current.body, diagnosis)}
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
