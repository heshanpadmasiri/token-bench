// Package ballerina implements Ballerina prompt metadata and project execution.
package ballerina

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type ballerina struct{}

type process struct {
	command *exec.Cmd
	output  lockedBuffer
	done    chan struct{}
	once    sync.Once
	stopErr error
}

// New initializes the private Ballerina implementation.
func New() *ballerina {
	return &ballerina{}
}

func (b *ballerina) Name() string {
	return "Ballerina"
}

func (b *ballerina) InitializeCommand() string {
	return "bal new ."
}

func (b *ballerina) TaskPromptSuffix() string {
	return ""
}

func (b *ballerina) StartProject(workingDir string, port int) (func() error, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid application port %d", port)
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
	if err := waitForHealth(running, port); err != nil {
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
