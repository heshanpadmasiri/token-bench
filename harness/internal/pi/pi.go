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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	provider = "openai-codex"
	model    = "gpt-5.6-sol"
	thinking = "high"
)

// TokenCount contains usage dimensions reported by Pi.
type TokenCount struct {
	Input      int `json:"input"`
	Output     int `json:"output"`
	Total      int `json:"total"`
	CacheRead  int `json:"cacheRead"`
	CacheWrite int `json:"cacheWrite"`
}

type pi struct {
	paneID    string
	stream    string
	events    chan map[string]json.RawMessage
	readErr   chan error
	cancel    context.CancelFunc
	sequence  atomic.Uint64
	mu        sync.Mutex
	stateMu   sync.RWMutex
	tokens    TokenCount
	lastReply string
}

// New initializes the private Pi implementation.
func New() *pi {
	return &pi{}
}

func (p *pi) Start(workingDir string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.paneID != "" {
		return errors.New("Pi is already started")
	}
	if _, err := exec.LookPath("tmux"); err != nil {
		return fmt.Errorf("find tmux: %w", err)
	}
	if _, err := exec.LookPath("pi"); err != nil {
		return fmt.Errorf("find pi: %w", err)
	}
	if info, err := os.Stat(workingDir); err != nil {
		return fmt.Errorf("inspect working directory: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("working directory %q is not a directory", workingDir)
	}

	stream, err := os.CreateTemp("", "token-bench-pi-*.jsonl")
	if err != nil {
		return fmt.Errorf("create Pi RPC stream: %w", err)
	}
	p.stream = stream.Name()
	if err := stream.Close(); err != nil {
		return fmt.Errorf("close Pi RPC stream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pane, err := command(ctx, "tmux", "split-window", "-P", "-F", "#{pane_id}", "-c", workingDir)
	if err != nil {
		_ = os.Remove(p.stream)
		p.stream = ""
		return fmt.Errorf("create tmux pane: %w", err)
	}
	p.paneID = strings.TrimSpace(pane)
	if p.paneID == "" {
		return errors.New("tmux returned an empty pane id")
	}

	readerCtx, readerCancel := context.WithCancel(context.Background())
	p.cancel = readerCancel
	p.events = make(chan map[string]json.RawMessage, 256)
	p.readErr = make(chan error, 1)
	go p.readEvents(readerCtx)

	startCommand := strings.Join([]string{
		"stty", "-icanon", "-echo", ";",
		"pi", "--mode", "rpc", "--no-session",
		"--provider", provider,
		"--model", model,
		"--thinking", thinking,
		"|", "tee", "-a", strconv.Quote(p.stream),
		";", "stty", "sane",
	}, " ")
	if err := sendShellCommand(ctx, p.paneID, startCommand); err != nil {
		_ = p.endLocked()
		return fmt.Errorf("start Pi: %w", err)
	}

	id := p.nextID()
	if err := p.send(ctx, id, map[string]any{"type": "get_state"}); err != nil {
		_ = p.endLocked()
		return err
	}
	if _, err := p.waitResponse(ctx, id); err != nil {
		_ = p.endLocked()
		return fmt.Errorf("wait for Pi RPC startup: %w", err)
	}
	return nil
}

func (p *pi) SendMessage(message string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paneID == "" {
		return "", errors.New("Pi is not started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	id := p.nextID()
	if err := p.send(ctx, id, map[string]any{"type": "prompt", "message": message}); err != nil {
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
	if p.paneID == "" {
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
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result error
	if p.paneID != "" {
		if _, err := command(ctx, "tmux", "kill-pane", "-t", p.paneID); err != nil {
			result = fmt.Errorf("kill Pi tmux pane: %w", err)
		}
		p.paneID = ""
	}
	if p.stream != "" {
		if err := os.Remove(p.stream); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("remove Pi RPC stream: %w", err))
		}
		p.stream = ""
	}
	return result
}

func (p *pi) send(ctx context.Context, id string, fields map[string]any) error {
	fields["id"] = id
	encoded, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("encode Pi RPC command: %w", err)
	}
	if err := sendRPCRecord(ctx, p.paneID, string(encoded)); err != nil {
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
	select {
	case event := <-p.events:
		return event, nil
	case err := <-p.readErr:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pi) readEvents(ctx context.Context) {
	file, err := os.Open(p.stream)
	if err != nil {
		p.reportReadError(fmt.Errorf("open Pi RPC stream: %w", err))
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var pending strings.Builder
	for {
		fragment, err := reader.ReadString('\n')
		pending.WriteString(fragment)
		if strings.HasSuffix(fragment, "\n") {
			line := strings.TrimSuffix(pending.String(), "\n")
			pending.Reset()
			if strings.HasSuffix(line, "\r") {
				line = strings.TrimSuffix(line, "\r")
			}
			if line != "" {
				var event map[string]json.RawMessage
				if decodeErr := json.Unmarshal([]byte(line), &event); decodeErr != nil {
					p.reportReadError(fmt.Errorf("decode Pi RPC event: %w", decodeErr))
					return
				}
				p.observe(event)
				select {
				case p.events <- event:
				case <-ctx.Done():
					return
				}
			}
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) {
			p.reportReadError(fmt.Errorf("read Pi RPC stream: %w", err))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
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

func (p *pi) reportReadError(err error) {
	select {
	case p.readErr <- err:
	default:
	}
}

func (p *pi) nextID() string {
	return fmt.Sprintf("token-bench-%d", p.sequence.Add(1))
}

func stringValue(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func sendShellCommand(ctx context.Context, paneID, value string) error {
	if err := sendLiteral(ctx, paneID, value); err != nil {
		return err
	}
	_, err := command(ctx, "tmux", "send-keys", "-t", paneID, "Enter")
	return err
}

func sendRPCRecord(ctx context.Context, paneID, value string) error {
	if err := sendLiteral(ctx, paneID, value); err != nil {
		return err
	}
	_, err := command(ctx, "tmux", "send-keys", "-t", paneID, "C-j")
	return err
}

func sendLiteral(ctx context.Context, paneID, value string) error {
	const chunkSize = 256
	characters := []rune(value)
	for start := 0; start < len(characters); start += chunkSize {
		end := min(start+chunkSize, len(characters))
		if _, err := command(ctx, "tmux", "send-keys", "-t", paneID, "-l", string(characters[start:end])); err != nil {
			return err
		}
	}
	return nil
}

func command(ctx context.Context, name string, args ...string) (string, error) {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}
