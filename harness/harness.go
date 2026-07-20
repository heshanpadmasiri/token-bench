// Package harness provide abstractions necessray to interact with a coding harness
package harness

import (
	"token-bench/harness/internal/claude"
	"token-bench/harness/internal/pi"
	"token-bench/harness/internal/shared"
)

type (
	// Config contains options shared by coding harness implementations.
	Config struct {
		SkillsDir string
	}

	Harness interface {
		Start(workingDir string) error
		// Send a message to the given harness. Message could be either the starting prompt or feedback
		SendMessage(message string) (string, error)
		TokenCount() (TokenCount, error)
		End() error
	}
	TokenCount = shared.TokenCount
)

// NewPi initializes the Pi coding harness.
func NewPi(config Config) Harness {
	return pi.New(pi.Config{SkillsDir: config.SkillsDir})
}

// NewClaude initializes the Claude Code harness.
func NewClaude(config Config) Harness {
	return claude.New(claude.Config{SkillsDir: config.SkillsDir})
}
