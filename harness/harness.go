// Package harness provide abstractions necessray to interact with a coding harness
package harness

import "token-bench/harness/internal/pi"

type (
	Harness interface {
		Start(workingDir string) error
		// Send a message to the given harness. Message could be either the starting prompt or feedback
		SendMessage(message string) (string, error)
		TokenCount() (TokenCount, error)
		End() error
	}
	TokenCount = pi.TokenCount
)

// NewPi initializes the Pi coding harness.
func NewPi() Harness {
	return pi.New()
}
