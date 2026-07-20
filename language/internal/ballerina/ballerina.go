// Package ballerina implements Ballerina project setup and execution.
package ballerina

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"text/template"
	"time"
)

//go:embed templates/*
var projectTemplates embed.FS

type ballerina struct {
	port int
}

type process struct {
	command *exec.Cmd
	output  lockedBuffer
	done    chan struct{}
	once    sync.Once
	stopErr error
}

type templateData struct {
	Port int
}

// New initializes the private Ballerina implementation.
func New() *ballerina {
	return &ballerina{}
}

func (b *ballerina) SetupProject(workingDir string, port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid application port %d", port)
	}
	if err := os.MkdirAll(workingDir, 0o755); err != nil {
		return fmt.Errorf("create Ballerina project: %w", err)
	}
	files := []struct {
		templatePath string
		outputPath   string
	}{
		{"templates/Ballerina.toml.tmpl", "Ballerina.toml"},
		{"templates/Config.toml.tmpl", "Config.toml"},
		{"templates/main.bal.tmpl", "main.bal"},
	}
	for _, file := range files {
		if err := render(file.templatePath, filepath.Join(workingDir, file.outputPath), templateData{Port: port}); err != nil {
			return err
		}
	}
	b.port = port
	return nil
}

func (b *ballerina) StartProject(workingDir string) (func() error, error) {
	if b.port == 0 {
		return nil, errors.New("Ballerina project is not set up")
	}
	if _, err := exec.LookPath("bal"); err != nil {
		return nil, fmt.Errorf("find bal: %w", err)
	}
	running := &process{done: make(chan struct{})}
	running.command = exec.Command("bal", "run")
	running.command.Dir = workingDir
	running.command.Stdout = &running.output
	running.command.Stderr = &running.output
	running.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := running.command.Start(); err != nil {
		return nil, fmt.Errorf("start bal run: %w", err)
	}
	go func() {
		_ = running.command.Wait()
		close(running.done)
	}()
	if err := waitForHealth(running, b.port); err != nil {
		_ = running.stop()
		return nil, fmt.Errorf("start Ballerina service: %w\n%s", err, running.output.String())
	}
	return running.stop, nil
}

func (p *process) stop() error {
	p.once.Do(func() {
		select {
		case <-p.done:
			return
		default:
		}
		pid := p.command.Process.Pid
		_ = syscall.Kill(-pid, syscall.SIGINT)
		select {
		case <-p.done:
			return
		case <-time.After(3 * time.Second):
		}
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			p.stopErr = fmt.Errorf("kill bal process: %w", err)
			return
		}
		select {
		case <-p.done:
		case <-time.After(3 * time.Second):
			p.stopErr = errors.New("timed out waiting for bal process to stop")
		}
	})
	return p.stopErr
}

func waitForHealth(running *process, port int) error {
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: time.Second}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	for {
		select {
		case <-running.done:
			return errors.New("bal run exited before the health endpoint became ready")
		default:
		}
		response, err := client.Get(endpoint)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-running.done:
			return errors.New("bal run exited before the health endpoint became ready")
		case <-deadline.C:
			return errors.New("timed out waiting for GET /health")
		case <-ticker.C:
		}
	}
}

func render(templatePath, outputPath string, data templateData) error {
	content, err := projectTemplates.ReadFile(templatePath)
	if err != nil {
		return fmt.Errorf("read template %s: %w", templatePath, err)
	}
	parsed, err := template.New(filepath.Base(templatePath)).Parse(string(content))
	if err != nil {
		return fmt.Errorf("parse template %s: %w", templatePath, err)
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, data); err != nil {
		return fmt.Errorf("render template %s: %w", templatePath, err)
	}
	if err := os.WriteFile(outputPath, output.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", outputPath, err)
	}
	return nil
}

type lockedBuffer struct {
	mu     sync.RWMutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.buffer.String()
}
