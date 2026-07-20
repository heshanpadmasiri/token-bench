// Package claude implements the interactive Claude Code harness.
package claude

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
	"time"

	"token-bench/harness/internal/shared"
)

const (
	model  = "opus"
	effort = "high"
)

// Config contains Claude Code startup configuration.
type Config struct {
	SkillsDir string
}

// TokenCount contains usage dimensions reported by Claude Code.
type TokenCount = shared.TokenCount

type claude struct {
	config         Config
	mu             sync.Mutex
	paneID         string
	settingsPath   string
	eventsPath     string
	pluginPath     string
	eventOffset    int64
	transcriptPath string
	tokens         TokenCount
	lastReply      string
}

// New initializes the private Claude Code implementation.
func New(config Config) *claude {
	return &claude{config: config}
}

func (c *claude) Start(workingDir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.paneID != "" {
		return errors.New("Claude is already started")
	}
	if _, err := exec.LookPath("tmux"); err != nil {
		return fmt.Errorf("find tmux: %w", err)
	}
	if _, err := exec.LookPath("claude"); err != nil {
		return fmt.Errorf("find claude: %w", err)
	}
	if info, err := os.Stat(workingDir); err != nil {
		return fmt.Errorf("inspect working directory: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("working directory %q is not a directory", workingDir)
	}
	if err := c.createSkillsPlugin(); err != nil {
		_ = c.cleanupFiles()
		return err
	}
	if err := c.createHookFiles(); err != nil {
		_ = c.cleanupFiles()
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pane, err := command(ctx, "tmux", "split-window", "-P", "-F", "#{pane_id}", "-c", workingDir)
	if err != nil {
		_ = c.cleanupFiles()
		return fmt.Errorf("create tmux pane: %w", err)
	}
	c.paneID = strings.TrimSpace(pane)
	if c.paneID == "" {
		_ = c.cleanupFiles()
		return errors.New("tmux returned an empty pane id")
	}
	if err := sendShellCommand(ctx, c.paneID, c.startCommand()); err != nil {
		_ = c.endLocked()
		return fmt.Errorf("start Claude: %w", err)
	}
	if err := c.waitUntilReady(ctx); err != nil {
		_ = c.endLocked()
		return fmt.Errorf("wait for Claude startup: %w", err)
	}
	return nil
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

func (c *claude) createHookFiles() error {
	events, err := os.CreateTemp("", "token-bench-claude-events-*.jsonl")
	if err != nil {
		return fmt.Errorf("create Claude hook stream: %w", err)
	}
	c.eventsPath = events.Name()
	if err := events.Close(); err != nil {
		_ = c.cleanupFiles()
		return fmt.Errorf("close Claude hook stream: %w", err)
	}

	settings, err := os.CreateTemp("", "token-bench-claude-settings-*.json")
	if err != nil {
		_ = c.cleanupFiles()
		return fmt.Errorf("create Claude settings: %w", err)
	}
	c.settingsPath = settings.Name()
	hookCommand := "cat >> " + shellQuote(c.eventsPath) + "; printf '\\n' >> " + shellQuote(c.eventsPath)
	configuration := map[string]any{
		"hooks": map[string]any{
			"Stop": []any{map[string]any{
				"hooks": []any{map[string]any{"type": "command", "command": hookCommand}},
			}},
		},
	}
	if err := json.NewEncoder(settings).Encode(configuration); err != nil {
		_ = settings.Close()
		_ = c.cleanupFiles()
		return fmt.Errorf("write Claude settings: %w", err)
	}
	if err := settings.Close(); err != nil {
		_ = c.cleanupFiles()
		return fmt.Errorf("close Claude settings: %w", err)
	}
	return nil
}

func (c *claude) startCommand() string {
	arguments := []string{
		"claude",
		"--model", shellQuote(model),
		"--effort", shellQuote(effort),
		"--dangerously-skip-permissions",
		"--setting-sources", "''",
		"--settings", shellQuote(c.settingsPath),
	}
	if c.pluginPath != "" {
		arguments = append(arguments, "--plugin-dir", shellQuote(c.pluginPath))
	}
	return strings.Join(arguments, " ")
}

func (c *claude) waitUntilReady(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		output, err := command(ctx, "tmux", "capture-pane", "-p", "-t", c.paneID)
		if err != nil {
			return err
		}
		if strings.Contains(output, "❯") {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *claude) SendMessage(message string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paneID == "" {
		return "", errors.New("Claude is not started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	if err := pasteMessage(ctx, c.paneID, message); err != nil {
		return "", fmt.Errorf("send Claude prompt: %w", err)
	}
	event, err := c.waitForStop(ctx)
	if err != nil {
		return "", fmt.Errorf("wait for Claude completion: %w", err)
	}
	if event.TranscriptPath == "" {
		return "", errors.New("Claude Stop hook did not provide a transcript path")
	}
	c.transcriptPath = event.TranscriptPath
	reply, tokens, err := parseTranscript(c.transcriptPath)
	if err != nil {
		return "", err
	}
	c.lastReply = reply
	c.tokens = tokens
	return c.lastReply, nil
}

type stopEvent struct {
	HookEventName  string `json:"hook_event_name"`
	TranscriptPath string `json:"transcript_path"`
}

func (c *claude) waitForStop(ctx context.Context) (stopEvent, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		file, err := os.Open(c.eventsPath)
		if err != nil {
			return stopEvent{}, fmt.Errorf("open Claude hook stream: %w", err)
		}
		if _, err := file.Seek(c.eventOffset, io.SeekStart); err != nil {
			_ = file.Close()
			return stopEvent{}, fmt.Errorf("seek Claude hook stream: %w", err)
		}
		reader := bufio.NewReader(file)
		for {
			line, readErr := reader.ReadString('\n')
			if strings.HasSuffix(line, "\n") {
				c.eventOffset += int64(len(line))
				var event stopEvent
				if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &event); err != nil {
					_ = file.Close()
					return stopEvent{}, fmt.Errorf("decode Claude Stop hook: %w", err)
				}
				if event.HookEventName == "Stop" {
					_ = file.Close()
					return event, nil
				}
			}
			if readErr != nil {
				if !errors.Is(readErr, io.EOF) {
					_ = file.Close()
					return stopEvent{}, fmt.Errorf("read Claude hook stream: %w", readErr)
				}
				break
			}
		}
		if err := file.Close(); err != nil {
			return stopEvent{}, fmt.Errorf("close Claude hook stream: %w", err)
		}
		select {
		case <-ctx.Done():
			return stopEvent{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func parseTranscript(path string) (string, TokenCount, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", TokenCount{}, fmt.Errorf("open Claude transcript: %w", err)
	}
	defer file.Close()

	type usage struct {
		Input      int `json:"input_tokens"`
		Output     int `json:"output_tokens"`
		CacheRead  int `json:"cache_read_input_tokens"`
		CacheWrite int `json:"cache_creation_input_tokens"`
	}
	type record struct {
		Type    string `json:"type"`
		UUID    string `json:"uuid"`
		Message struct {
			ID      string          `json:"id"`
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
			Usage   *usage          `json:"usage"`
		} `json:"message"`
	}

	usages := make(map[string]usage)
	lastReply := ""
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var current record
		if err := json.Unmarshal(scanner.Bytes(), &current); err != nil {
			return "", TokenCount{}, fmt.Errorf("decode Claude transcript: %w", err)
		}
		if current.Type != "assistant" || current.Message.Role != "assistant" {
			continue
		}
		if current.Message.Usage != nil {
			key := current.Message.ID
			if key == "" {
				key = current.UUID
			}
			usages[key] = *current.Message.Usage
		}
		var blocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(current.Message.Content, &blocks); err != nil {
			return "", TokenCount{}, fmt.Errorf("decode Claude assistant content: %w", err)
		}
		var text strings.Builder
		for _, block := range blocks {
			if block.Type == "text" {
				text.WriteString(block.Text)
			}
		}
		if text.Len() > 0 {
			lastReply = text.String()
		}
	}
	if err := scanner.Err(); err != nil {
		return "", TokenCount{}, fmt.Errorf("read Claude transcript: %w", err)
	}
	var tokens TokenCount
	for _, current := range usages {
		tokens.Input += current.Input
		tokens.Output += current.Output
		tokens.CacheRead += current.CacheRead
		tokens.CacheWrite += current.CacheWrite
	}
	tokens.Total = tokens.Input + tokens.Output + tokens.CacheRead + tokens.CacheWrite
	return lastReply, tokens, nil
}

func (c *claude) TokenCount() (TokenCount, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paneID == "" {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result error
	if c.paneID != "" {
		if _, err := command(ctx, "tmux", "kill-pane", "-t", c.paneID); err != nil {
			result = fmt.Errorf("kill Claude tmux pane: %w", err)
		}
		c.paneID = ""
	}
	return errors.Join(result, c.cleanupFiles())
}

func (c *claude) cleanupFiles() error {
	var result error
	for label, path := range map[string]string{"hook stream": c.eventsPath, "settings": c.settingsPath} {
		if path == "" {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("remove Claude %s: %w", label, err))
		}
	}
	if c.pluginPath != "" {
		if err := os.RemoveAll(c.pluginPath); err != nil {
			result = errors.Join(result, fmt.Errorf("remove Claude skills plugin: %w", err))
		}
	}
	c.eventsPath = ""
	c.settingsPath = ""
	c.pluginPath = ""
	return result
}

func pasteMessage(ctx context.Context, paneID, message string) error {
	file, err := os.CreateTemp("", "token-bench-claude-prompt-*.txt")
	if err != nil {
		return err
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err := file.WriteString(message); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	buffer := "token-bench-claude-" + strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if _, err := command(ctx, "tmux", "load-buffer", "-b", buffer, path); err != nil {
		return err
	}
	if _, err := command(ctx, "tmux", "paste-buffer", "-d", "-b", buffer, "-t", paneID); err != nil {
		return err
	}
	_, err = command(ctx, "tmux", "send-keys", "-t", paneID, "Enter")
	return err
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func sendShellCommand(ctx context.Context, paneID, value string) error {
	if _, err := command(ctx, "tmux", "send-keys", "-t", paneID, "-l", value); err != nil {
		return err
	}
	_, err := command(ctx, "tmux", "send-keys", "-t", paneID, "Enter")
	return err
}

func command(ctx context.Context, name string, args ...string) (string, error) {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}
