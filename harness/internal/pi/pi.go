// Package pi implements the Pi coding harness.
package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"token-bench/harness/internal/shared"
	"token-bench/tracing"
)

const (
	provider           = "openai-codex"
	model              = "gpt-5.6-sol"
	thinking           = "high"
	streamArtifactName = "pi-stream.jsonl"
	stderrArtifactName = "pi-stderr.log"
)

// Config contains Pi startup configuration.
type Config struct {
	SkillsDir string
}

// TokenCount contains usage dimensions reported by Pi.
type TokenCount = shared.TokenCount

type pi struct {
	config    Config
	process   *runningProcess
	sequence  atomic.Uint64
	mu        sync.Mutex
	stateMu   sync.RWMutex
	tokens    TokenCount
	lastReply string
}

type runningProcess struct {
	command     *exec.Cmd
	stdin       io.WriteCloser
	events      chan map[string]json.RawMessage
	stopping    chan struct{}
	stopOnce    sync.Once
	processDone *asyncStatus
	readerDone  *asyncStatus
	streamFile  *os.File
	stderrFile  *os.File
	streamPath  string
	stderrPath  string
	workingDir  string
}

type asyncStatus struct {
	done chan struct{}
	err  error
}

func newAsyncStatus() *asyncStatus {
	return &asyncStatus{done: make(chan struct{})}
}

func (s *asyncStatus) complete(err error) {
	s.err = err
	close(s.done)
}

func (s *asyncStatus) result() error {
	<-s.done
	return s.err
}

// New initializes the private Pi implementation.
func New(config Config) *pi {
	return &pi{config: config}
}

func (p *pi) Model() string {
	return model
}

func (p *pi) ThinkingBudget() string {
	return thinking
}

func (p *pi) Start(workingDir string, benchmark tracing.BenchmarkSpan) error {
	if benchmark != nil {
		panic("tracing is not supported by the Pi harness")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.process != nil {
		return errors.New("Pi is already started")
	}
	executable, err := exec.LookPath("pi")
	if err != nil {
		return fmt.Errorf("find pi: %w", err)
	}
	if info, err := os.Stat(workingDir); err != nil {
		return fmt.Errorf("inspect working directory: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("working directory %q is not a directory", workingDir)
	}

	running, err := p.startProcess(executable, workingDir)
	if err != nil {
		return err
	}
	p.process = running
	p.stateMu.Lock()
	p.tokens = TokenCount{}
	p.lastReply = ""
	p.stateMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := p.nextID()
	if err := p.send(id, map[string]any{"type": "get_state"}); err != nil {
		_ = p.endLocked()
		return err
	}
	if _, err := p.waitResponse(ctx, id); err != nil {
		_ = p.endLocked()
		return fmt.Errorf("wait for Pi RPC startup: %w", err)
	}
	return nil
}

func (p *pi) startProcess(executable, workingDir string) (*runningProcess, error) {
	streamFile, err := os.CreateTemp("", "token-bench-pi-stream-*.jsonl")
	if err != nil {
		return nil, fmt.Errorf("create Pi stream log: %w", err)
	}
	stderrFile, err := os.CreateTemp("", "token-bench-pi-stderr-*.log")
	if err != nil {
		_ = streamFile.Close()
		_ = os.Remove(streamFile.Name())
		return nil, fmt.Errorf("create Pi stderr log: %w", err)
	}

	command := exec.Command(executable, p.startArguments()...)
	command.Dir = workingDir
	command.Stderr = stderrFile
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, p.cleanupUnstartedProcess(streamFile, stderrFile, workingDir, fmt.Errorf("open Pi stdin: %w", err))
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, p.cleanupUnstartedProcess(streamFile, stderrFile, workingDir, fmt.Errorf("open Pi stdout: %w", err))
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, p.cleanupUnstartedProcess(streamFile, stderrFile, workingDir, fmt.Errorf("start Pi: %w", err))
	}

	running := &runningProcess{
		command:     command,
		stdin:       stdin,
		events:      make(chan map[string]json.RawMessage, 256),
		stopping:    make(chan struct{}),
		processDone: newAsyncStatus(),
		readerDone:  newAsyncStatus(),
		streamFile:  streamFile,
		stderrFile:  stderrFile,
		streamPath:  streamFile.Name(),
		stderrPath:  stderrFile.Name(),
		workingDir:  workingDir,
	}
	go func() {
		// StdoutPipe must be drained before Wait so the final event and log tail
		// cannot be truncated when Pi exits.
		<-running.readerDone.done
		running.processDone.complete(command.Wait())
	}()
	go p.readStream(io.TeeReader(stdout, streamFile), running)
	return running, nil
}

func (p *pi) cleanupUnstartedProcess(streamFile, stderrFile *os.File, workingDir string, cause error) error {
	streamPath := streamFile.Name()
	stderrPath := stderrFile.Name()
	closeErr := errors.Join(streamFile.Close(), stderrFile.Close())
	persistErr := errors.Join(
		persistArtifact(streamPath, filepath.Join(workingDir, streamArtifactName)),
		persistArtifact(stderrPath, filepath.Join(workingDir, stderrArtifactName)),
	)
	return errors.Join(cause, closeErr, persistErr)
}

func (p *pi) startArguments() []string {
	arguments := []string{
		"--mode", "rpc",
		"--no-session",
		"--no-skills",
		"--provider", provider,
		"--model", model,
		"--thinking", thinking,
	}
	if p.config.SkillsDir != "" {
		arguments = append(arguments, "--skill", p.config.SkillsDir)
	}
	return arguments
}

func (p *pi) SendMessage(message string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.process == nil {
		return "", errors.New("Pi is not started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	id := p.nextID()
	if err := p.send(id, map[string]any{"type": "prompt", "message": message}); err != nil {
		return "", err
	}
	if _, err := p.waitResponse(ctx, id); err != nil {
		return "", fmt.Errorf("send Pi prompt: %w", err)
	}
	if err := p.waitEvent(ctx, "agent_settled"); err != nil {
		return "", fmt.Errorf("wait for Pi completion: %w", err)
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.lastReply, nil
}

func (p *pi) TokenCount() (TokenCount, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.process == nil {
		return TokenCount{}, errors.New("Pi is not started")
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.tokens, nil
}

func (p *pi) End() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.endLocked()
}

func (p *pi) endLocked() error {
	if p.process == nil {
		return nil
	}
	running := p.process
	running.stopOnce.Do(func() { close(running.stopping) })

	var result error
	if err := running.stdin.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close Pi stdin: %w", err))
	}
	select {
	case <-running.processDone.done:
	case <-time.After(10 * time.Second):
		result = errors.Join(result, errors.New("Pi did not exit within 10s"))
		if err := running.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			result = errors.Join(result, fmt.Errorf("kill Pi: %w", err))
		}
		<-running.processDone.done
	}
	if err := running.processDone.result(); err != nil {
		result = errors.Join(result, fmt.Errorf("wait for Pi: %w", err))
	}

	select {
	case <-running.readerDone.done:
		if err := running.readerDone.result(); err != nil {
			result = errors.Join(result, err)
		}
	case <-time.After(2 * time.Second):
		result = errors.Join(result, errors.New("Pi stream reader did not stop within 2s"))
	}
	if err := running.streamFile.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close Pi stream log: %w", err))
	}
	if err := running.stderrFile.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close Pi stderr log: %w", err))
	}
	if err := persistArtifact(running.streamPath, filepath.Join(running.workingDir, streamArtifactName)); err != nil {
		result = errors.Join(result, fmt.Errorf("persist Pi stream log: %w", err))
	}
	if err := persistArtifact(running.stderrPath, filepath.Join(running.workingDir, stderrArtifactName)); err != nil {
		result = errors.Join(result, fmt.Errorf("persist Pi stderr log: %w", err))
	}
	p.process = nil
	return result
}

func (p *pi) send(id string, fields map[string]any) error {
	fields["id"] = id
	if err := json.NewEncoder(p.process.stdin).Encode(fields); err != nil {
		return fmt.Errorf("send Pi RPC command: %w", err)
	}
	return nil
}

func (p *pi) waitResponse(ctx context.Context, id string) (map[string]json.RawMessage, error) {
	for {
		event, err := p.nextEvent(ctx)
		if err != nil {
			return nil, err
		}
		if stringValue(event["type"]) != "response" {
			continue
		}
		if stringValue(event["command"]) == "parse" {
			return nil, errors.New(stringValue(event["error"]))
		}
		if stringValue(event["id"]) != id {
			continue
		}
		var success bool
		if err := json.Unmarshal(event["success"], &success); err != nil {
			return nil, fmt.Errorf("decode Pi RPC response: %w", err)
		}
		if !success {
			return nil, errors.New(stringValue(event["error"]))
		}
		return event, nil
	}
}

func (p *pi) waitEvent(ctx context.Context, eventType string) error {
	for {
		event, err := p.nextEvent(ctx)
		if err != nil {
			return err
		}
		if stringValue(event["type"]) == eventType {
			return nil
		}
	}
}

func (p *pi) nextEvent(ctx context.Context) (map[string]json.RawMessage, error) {
	running := p.process
	select {
	case event, ok := <-running.events:
		if !ok {
			return nil, p.processFailure("Pi RPC stream ended before the expected event", running)
		}
		return event, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pi) readStream(reader io.Reader, running *runningProcess) {
	var readErr error
	defer func() {
		running.readerDone.complete(readErr)
		close(running.events)
	}()

	buffered := bufio.NewReader(reader)
	for {
		line, err := buffered.ReadString('\n')
		if line != "" {
			line = strings.TrimSuffix(line, "\n")
			line = strings.TrimSuffix(line, "\r")
			if line != "" {
				var event map[string]json.RawMessage
				if decodeErr := json.Unmarshal([]byte(line), &event); decodeErr != nil {
					readErr = fmt.Errorf("decode Pi RPC event: %w", decodeErr)
					return
				}
				p.observe(event)
				select {
				case running.events <- event:
				case <-running.stopping:
				}
			}
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return
		}
		readErr = fmt.Errorf("read Pi RPC stream: %w", err)
		return
	}
}

func (p *pi) observe(event map[string]json.RawMessage) {
	if stringValue(event["type"]) != "message_end" {
		return
	}
	var message struct {
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage TokenCount `json:"usage"`
	}
	if err := json.Unmarshal(event["message"], &message); err != nil || message.Role != "assistant" {
		return
	}
	var reply strings.Builder
	for _, block := range message.Content {
		if block.Type == "text" {
			reply.WriteString(block.Text)
		}
	}
	p.stateMu.Lock()
	p.lastReply = reply.String()
	p.tokens.Input += message.Usage.Input
	p.tokens.Output += message.Usage.Output
	p.tokens.CacheRead += message.Usage.CacheRead
	p.tokens.CacheWrite += message.Usage.CacheWrite
	p.tokens.Total += message.Usage.Input + message.Usage.Output + message.Usage.CacheRead + message.Usage.CacheWrite
	p.stateMu.Unlock()
}

func (p *pi) processFailure(prefix string, running *runningProcess) error {
	parts := []string{prefix}
	select {
	case <-running.readerDone.done:
		if err := running.readerDone.result(); err != nil {
			parts = append(parts, err.Error())
		}
	default:
	}
	select {
	case <-running.processDone.done:
		if err := running.processDone.result(); err != nil {
			parts = append(parts, err.Error())
		}
	default:
	}
	if err := running.stderrFile.Sync(); err == nil {
		if content, readErr := os.ReadFile(running.stderrPath); readErr == nil {
			if stderr := strings.TrimSpace(string(content)); stderr != "" {
				parts = append(parts, stderr)
			}
		}
	}
	return errors.New(strings.Join(parts, ": "))
}

func (p *pi) nextID() string {
	return fmt.Sprintf("token-bench-%d", p.sequence.Add(1))
}

func stringValue(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func persistArtifact(source, destination string) error {
	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(source, destination); err == nil {
		return nil
	}

	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	return os.Remove(source)
}
