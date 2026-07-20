// Package language provides abstractions necessary to setup a benchmark in that given language. These setup commands are used to remove
// token cost of repeated validations
package language

import (
	"token-bench/language/internal/ballerina"
	"token-bench/language/internal/golang"
)

type (
	Language interface {
		SetupProject(workingDir string, port int) error
		StartProject(workingDir string) (func() error, error)
	}
)

// NewBallerina initializes the Ballerina language implementation.
func NewBallerina() Language {
	return ballerina.New()
}

// NewGo initializes the Go language implementation.
func NewGo() Language {
	return golang.New()
}
