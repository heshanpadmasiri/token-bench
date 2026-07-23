// Package result defines the persisted result of one benchmark run.
package result

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	SchemaVersion       = 3
	legacySchemaVersion = 2
	unknownModel        = "unknown-model"
	unknownThinking     = "unknown-thinking"
)

// TokenCount contains the token usage dimensions recorded for a run.
type TokenCount struct {
	Input      int `json:"input"`
	Output     int `json:"output"`
	Total      int `json:"total"`
	CacheRead  int `json:"cacheRead"`
	CacheWrite int `json:"cacheWrite"`
}

// Result is the persisted outcome and normalized CLI configuration for one run.
type Result struct {
	SchemaVersion  int        `json:"schemaVersion"`
	Task           string     `json:"task"`
	Language       string     `json:"language"`
	Harness        string     `json:"harness"`
	Model          string     `json:"model"`
	ThinkingBudget string     `json:"thinkingBudget"`
	SkillsDir      string     `json:"skillsDir,omitempty"`
	PromptTemplate string     `json:"promptTemplate,omitempty"`
	Run            int        `json:"run"`
	RequestedRuns  int        `json:"requestedRuns"`
	Passed         bool       `json:"passed"`
	Corrections    int        `json:"corrections"`
	Tokens         TokenCount `json:"tokens"`
	Error          string     `json:"error,omitempty"`
}

// Write persists one result as indented JSON.
func Write(path string, value Result) error {
	if err := value.Validate(); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o644)
}

// Read decodes and validates one persisted result.
func Read(path string) (Result, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Result{}, err
	}
	var value Result
	if err := json.Unmarshal(content, &value); err != nil {
		return Result{}, err
	}
	if value.SchemaVersion == legacySchemaVersion {
		if value.Model == "" {
			value.Model = unknownModel
		}
		if value.ThinkingBudget == "" {
			value.ThinkingBudget = unknownThinking
		}
	}
	if err := value.Validate(); err != nil {
		return Result{}, err
	}
	return value, nil
}

// Validate checks the persisted schema and required metadata.
func (r Result) Validate() error {
	if r.SchemaVersion != legacySchemaVersion && r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported result schema version %d", r.SchemaVersion)
	}
	if r.Task == "" || r.Language == "" || r.Harness == "" {
		return errors.New("task, language, and harness are required")
	}
	if r.SchemaVersion == SchemaVersion && (r.Model == "" || r.ThinkingBudget == "") {
		return errors.New("model and thinkingBudget are required")
	}
	if r.SkillsDir != "" && !filepath.IsAbs(r.SkillsDir) {
		return errors.New("skillsDir must be an absolute path")
	}
	if r.Run < 1 || r.RequestedRuns < 1 || r.Run > r.RequestedRuns {
		return errors.New("run must be within requestedRuns")
	}
	if r.Corrections < 0 {
		return errors.New("corrections cannot be negative")
	}
	return nil
}
