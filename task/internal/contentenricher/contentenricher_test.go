package contentenricher

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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPromptIncludesDatabaseSchemaAndDownstreamContract(t *testing.T) {
	prompt, err := New([]int{19080, 19081, 19082}).Prompt()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"Host: `127.0.0.1`", "Port: `19081`", "Database: `tokenbench`",
		"Username: `tokenbench`", "Password: `tokenbench`", "Table: `user_addresses`",
		"http://127.0.0.1:19082/enriched-messages", "postal_code",
		"top-level `address`", "HTTP 202 with an empty body", "already running and are owned by the benchmark",
		"Never stop, kill, replace, reconfigure, or otherwise modify either backend",
	} {
		if !strings.Contains(prompt, expected) {
			t.Errorf("prompt does not contain %q", expected)
		}
	}
}

func TestPostgreSQLFeedbackValidationAndCleanup(t *testing.T) {
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

	var city string
	if err := handle.database.QueryRow(context.Background(), "SELECT city FROM user_addresses WHERE user_id=$1", "user-1042").Scan(&city); err != nil {
		t.Fatal(err)
	}
	if city != "London" {
		t.Fatalf("unexpected seeded city %q", city)
	}

	stopApplication := startApplication(t, ports[0], handle.databaseURL(), origin(ports[2])+"/enriched-messages")
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
		t.Fatalf("PostgreSQL container %q was not removed", containerName)
	}
}

func TestNewRejectsInvalidPorts(t *testing.T) {
	for _, ports := range [][]int{{19080}, {19080, 19081, 0}, {19080, 19081, 65536}} {
		if _, err := New(ports).Prompt(); err == nil {
			t.Errorf("New(%v) did not reject invalid ports", ports)
		}
	}
}

func startApplication(t *testing.T, applicationPort int, databaseURL, downstreamURL string) func() {
	t.Helper()
	database, err := pgx.Connect(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", applicationPort))
	if err != nil {
		_ = database.Close(context.Background())
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /messages", func(writer http.ResponseWriter, request *http.Request) {
		var message map[string]any
		decoder := json.NewDecoder(request.Body)
		if decoder.Decode(&message) != nil || message == nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var trailing any
		if !errors.Is(decoder.Decode(&trailing), io.EOF) {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, exists := message["address"]; exists {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		user, ok := message["user"].(map[string]any)
		if !ok {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, exists := user["address"]; exists {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		userID, ok := user["id"].(string)
		if !ok || userID == "" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var found address
		err := database.QueryRow(request.Context(), `SELECT street, city, state, postal_code, country FROM user_addresses WHERE user_id=$1`, userID).
			Scan(&found.Street, &found.City, &found.State, &found.PostalCode, &found.Country)
		if errors.Is(err, pgx.ErrNoRows) {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		message["address"] = found
		body, err := json.Marshal(message)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		outgoing, err := http.NewRequestWithContext(request.Context(), http.MethodPost, downstreamURL, bytes.NewReader(body))
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		outgoing.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(outgoing)
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			writer.WriteHeader(http.StatusAccepted)
			return
		}
		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		writer.WriteHeader(response.StatusCode)
		_, _ = writer.Write(responseBody)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()

	var once sync.Once
	return func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = server.Shutdown(ctx)
			_ = database.Close(ctx)
		})
	}
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
