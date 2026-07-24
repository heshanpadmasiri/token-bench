// Package claude implements the Claude Code streaming harness.
package claude

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"token-bench/harness/internal/shared"
	"token-bench/tracing"
)

const (
	model              = "opus"
	effort             = "high"
	streamArtifactName = "claude-stream.jsonl"
	stderrArtifactName = "claude-stderr.log"
)

// Config contains Claude Code startup configuration.
type Config struct {
	SkillsDir string
	Status    chan string
}

// TokenCount contains usage dimensions reported by Claude Code.
type TokenCount = shared.TokenCount

type claude struct {
	config     Config
	mu         sync.Mutex
	process    *runningProcess
	pluginPath string
	tokens     TokenCount
	lastReply  string
}

type runningProcess struct {
	command     *exec.Cmd
	stdin       io.WriteCloser
	results     chan resultEvent
	stopping    chan struct{}
	stopOnce    sync.Once
	processDone *asyncStatus
	readerDone  *asyncStatus
	streamFile  *os.File
	stderrFile  *os.File
	streamPath  string
	stderrPath  string
	workingDir  string
	trace       *traceAdapter
	status      chan string
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

type resultEvent struct {
	Type       string                `json:"type"`
	Subtype    string                `json:"subtype"`
	Result     string                `json:"result"`
	IsError    bool                  `json:"is_error"`
	Errors     []string              `json:"errors"`
	Usage      resultUsage           `json:"usage"`
	ModelUsage map[string]modelUsage `json:"modelUsage"`
	Origin     struct {
		Kind string `json:"kind"`
	} `json:"origin"`
}

type resultUsage struct {
	Input      int `json:"input_tokens"`
	Output     int `json:"output_tokens"`
	CacheRead  int `json:"cache_read_input_tokens"`
	CacheWrite int `json:"cache_creation_input_tokens"`
}

type modelUsage struct {
	Input      int `json:"inputTokens"`
	Output     int `json:"outputTokens"`
	CacheRead  int `json:"cacheReadInputTokens"`
	CacheWrite int `json:"cacheCreationInputTokens"`
}

// New initializes the private Claude Code implementation.
func New(config Config) *claude {
	return &claude{config: config}
}

func (c *claude) Model() string {
	return model
}

func (c *claude) ThinkingBudget() string {
	return effort
}

func (c *claude) Start(workingDir string, benchmark tracing.BenchmarkSpan) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.process != nil {
		return errors.New("Claude is already started")
	}
	executable, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("find claude: %w", err)
	}
	if info, err := os.Stat(workingDir); err != nil {
		return fmt.Errorf("inspect working directory: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("working directory %q is not a directory", workingDir)
	}
	if err := c.createSkillsPlugin(); err != nil {
		_ = c.cleanupPlugin()
		return err
	}

	running, err := c.startProcess(executable, workingDir, benchmark)
	if err != nil {
		_ = c.cleanupPlugin()
		return err
	}
	c.process = running
	c.tokens = TokenCount{}
	c.lastReply = ""
	return nil
}

func (c *claude) startProcess(executable, workingDir string, benchmark tracing.BenchmarkSpan) (*runningProcess, error) {
	streamFile, err := os.CreateTemp("", "token-bench-claude-stream-*.jsonl")
	if err != nil {
		return nil, fmt.Errorf("create Claude stream log: %w", err)
	}
	stderrFile, err := os.CreateTemp("", "token-bench-claude-stderr-*.log")
	if err != nil {
		_ = streamFile.Close()
		_ = os.Remove(streamFile.Name())
		return nil, fmt.Errorf("create Claude stderr log: %w", err)
	}

	command := exec.Command(executable, c.startArguments()...)
	command.Dir = workingDir
	command.Env = append(os.Environ(), "CLAUDE_CODE_DISABLE_AUTO_MEMORY=1")
	command.Stderr = stderrFile
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, c.cleanupUnstartedProcess(streamFile, stderrFile, workingDir, fmt.Errorf("open Claude stdin: %w", err))
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, c.cleanupUnstartedProcess(streamFile, stderrFile, workingDir, fmt.Errorf("open Claude stdout: %w", err))
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, c.cleanupUnstartedProcess(streamFile, stderrFile, workingDir, fmt.Errorf("start Claude: %w", err))
	}

	running := &runningProcess{
		command:     command,
		stdin:       stdin,
		results:     make(chan resultEvent, 64),
		stopping:    make(chan struct{}),
		processDone: newAsyncStatus(),
		readerDone:  newAsyncStatus(),
		streamFile:  streamFile,
		stderrFile:  stderrFile,
		streamPath:  streamFile.Name(),
		stderrPath:  stderrFile.Name(),
		workingDir:  workingDir,
		trace:       newTraceAdapter(benchmark, c.Model()),
		status:      c.config.Status,
	}
	go func() {
		// StdoutPipe must be drained before Wait so the final result and log tail
		// cannot be truncated when Claude exits.
		<-running.readerDone.done
		running.processDone.complete(command.Wait())
	}()
	go readStream(io.TeeReader(stdout, streamFile), running)
	return running, nil
}

func (c *claude) cleanupUnstartedProcess(streamFile, stderrFile *os.File, workingDir string, cause error) error {
	streamPath := streamFile.Name()
	stderrPath := stderrFile.Name()
	closeErr := errors.Join(streamFile.Close(), stderrFile.Close())
	persistErr := errors.Join(
		persistArtifact(streamPath, filepath.Join(workingDir, streamArtifactName)),
		persistArtifact(stderrPath, filepath.Join(workingDir, stderrArtifactName)),
	)
	return errors.Join(cause, closeErr, persistErr)
}

func readStream(reader io.Reader, running *runningProcess) {
	var readErr error
	defer func() {
		running.trace.Close()
		running.readerDone.complete(readErr)
		close(running.results)
	}()

	decoder := json.NewDecoder(reader)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			readErr = fmt.Errorf("decode Claude stream: %w", err)
			return
		}

		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			readErr = fmt.Errorf("decode Claude stream envelope: %w", err)
			return
		}
		if err := running.trace.Process(envelope.Type, raw); err != nil {
			readErr = fmt.Errorf("trace Claude stream event: %w", err)
			return
		}
		publishStatus(running.status, envelope.Type, raw)
		if envelope.Type != "result" {
			continue
		}
		var result resultEvent
		if err := json.Unmarshal(raw, &result); err != nil {
			readErr = fmt.Errorf("decode Claude result: %w", err)
			return
		}
		select {
		case running.results <- result:
		case <-running.stopping:
		}
	}
}

func (c *claude) createSkillsPlugin() error {
	if c.config.SkillsDir == "" {
		return nil
	}
	plugin, err := os.MkdirTemp("", "token-bench-claude-plugin-*")
	if err != nil {
		return fmt.Errorf("create Claude skills plugin: %w", err)
	}
	c.pluginPath = plugin
	manifestDir := filepath.Join(plugin, ".claude-plugin")
	if err := os.Mkdir(manifestDir, 0o755); err != nil {
		return fmt.Errorf("create Claude plugin manifest directory: %w", err)
	}
	manifest := []byte(`{"name":"token-bench-skills","version":"1.0.0","description":"Skills supplied to token-bench","author":{"name":"token-bench"}}` + "\n")
	if err := os.WriteFile(filepath.Join(manifestDir, "plugin.json"), manifest, 0o600); err != nil {
		return fmt.Errorf("write Claude plugin manifest: %w", err)
	}
	skillsDir := filepath.Join(plugin, "skills")
	if err := os.Mkdir(skillsDir, 0o755); err != nil {
		return fmt.Errorf("create Claude plugin skills directory: %w", err)
	}

	sources, err := findSkillDirectories(c.config.SkillsDir)
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return fmt.Errorf("skills directory %q contains no SKILL.md files", c.config.SkillsDir)
	}
	for _, source := range sources {
		relative, err := filepath.Rel(c.config.SkillsDir, source)
		if err != nil {
			return fmt.Errorf("name Claude skill %q: %w", source, err)
		}
		name := filepath.Base(c.config.SkillsDir)
		if relative != "." {
			name = strings.ReplaceAll(relative, string(filepath.Separator), "--")
		}
		if err := os.Symlink(source, filepath.Join(skillsDir, name)); err != nil {
			return fmt.Errorf("stage Claude skill %q: %w", source, err)
		}
	}
	return nil
}

func findSkillDirectories(root string) ([]string, error) {
	if _, err := os.Stat(filepath.Join(root, "SKILL.md")); err == nil {
		return []string{root}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect skill in %q: %w", root, err)
	}
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == "SKILL.md" {
			directories = append(directories, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover Claude skills: %w", err)
	}
	return directories, nil
}

func (c *claude) startArguments() []string {
	arguments := []string{
		"--model", model,
		"--effort", effort,
		"--dangerously-skip-permissions",
		"--setting-sources", "",
		"-p",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
	}
	if c.pluginPath != "" {
		arguments = append(arguments, "--plugin-dir", c.pluginPath)
	}
	return arguments
}

func (c *claude) SendMessage(message string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.process == nil {
		return "", errors.New("Claude is not started")
	}

	record := struct {
		Type    string `json:"type"`
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		ParentToolUseID *string `json:"parent_tool_use_id"`
	}{Type: "user"}
	record.Message.Role = "user"
	record.Message.Content = message
	c.process.trace.SetNextInput(message)
	if err := json.NewEncoder(c.process.stdin).Encode(record); err != nil {
		return "", fmt.Errorf("send Claude prompt: %w", err)
	}

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		select {
		case result, ok := <-c.process.results:
			if !ok {
				return "", c.processFailure("Claude stream ended before a result", c.process)
			}
			c.addUsage(result)
			if result.Origin.Kind == "task-notification" {
				continue
			}
			if result.Subtype != "success" || result.IsError {
				detail := strings.Join(result.Errors, "; ")
				if detail == "" {
					detail = "Claude reported an unsuccessful result"
				}
				return "", fmt.Errorf("Claude result %q: %s", result.Subtype, detail)
			}
			c.lastReply = result.Result
			return c.lastReply, nil
		case <-timer.C:
			return "", errors.New("wait for Claude completion: timed out after 1h")
		}
	}
}

func (c *claude) addUsage(result resultEvent) {
	if len(result.ModelUsage) == 0 {
		c.tokens.Input += result.Usage.Input
		c.tokens.Output += result.Usage.Output
		c.tokens.CacheRead += result.Usage.CacheRead
		c.tokens.CacheWrite += result.Usage.CacheWrite
	} else {
		for _, usage := range result.ModelUsage {
			c.tokens.Input += usage.Input
			c.tokens.Output += usage.Output
			c.tokens.CacheRead += usage.CacheRead
			c.tokens.CacheWrite += usage.CacheWrite
		}
	}
	c.tokens.Total = c.tokens.Input + c.tokens.Output + c.tokens.CacheRead + c.tokens.CacheWrite
}

func (c *claude) TokenCount() (TokenCount, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.process == nil {
		return TokenCount{}, errors.New("Claude is not started")
	}
	return c.tokens, nil
}

func (c *claude) End() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.endLocked()
}

func (c *claude) endLocked() error {
	if c.process == nil {
		return c.cleanupPlugin()
	}
	running := c.process
	running.stopOnce.Do(func() { close(running.stopping) })

	var result error
	if err := running.stdin.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close Claude stdin: %w", err))
	}
	select {
	case <-running.processDone.done:
	case <-time.After(10 * time.Second):
		result = errors.Join(result, errors.New("Claude did not exit within 10s"))
		if err := running.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			result = errors.Join(result, fmt.Errorf("kill Claude: %w", err))
		}
		<-running.processDone.done
	}
	if err := running.processDone.result(); err != nil {
		result = errors.Join(result, fmt.Errorf("wait for Claude: %w", err))
	}

	select {
	case <-running.readerDone.done:
		if err := running.readerDone.result(); err != nil {
			result = errors.Join(result, err)
		}
	case <-time.After(2 * time.Second):
		result = errors.Join(result, errors.New("Claude stream reader did not stop within 2s"))
	}
	if err := running.streamFile.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close Claude stream log: %w", err))
	}
	if err := running.stderrFile.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close Claude stderr log: %w", err))
	}
	if err := persistArtifact(running.streamPath, filepath.Join(running.workingDir, streamArtifactName)); err != nil {
		result = errors.Join(result, fmt.Errorf("persist Claude stream log: %w", err))
	}
	if err := persistArtifact(running.stderrPath, filepath.Join(running.workingDir, stderrArtifactName)); err != nil {
		result = errors.Join(result, fmt.Errorf("persist Claude stderr log: %w", err))
	}
	c.process = nil
	return errors.Join(result, c.cleanupPlugin())
}

func (c *claude) processFailure(prefix string, running *runningProcess) error {
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

func (c *claude) cleanupPlugin() error {
	if c.pluginPath == "" {
		return nil
	}
	err := os.RemoveAll(c.pluginPath)
	c.pluginPath = ""
	if err != nil {
		return fmt.Errorf("remove Claude skills plugin: %w", err)
	}
	return nil
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
