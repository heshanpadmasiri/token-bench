// Package language provides abstractions necessary to setup a benchmark in that given language. These setup commands are used to remove
// token cost of repeated validations
package language

import (
	"token-bench/language/internal/ballerina"
	"token-bench/language/internal/golang"
	"token-bench/language/internal/java"
	"token-bench/language/internal/python"
)

type (
	Language interface {
		Name() string
		InitializeCommand() string
		TaskPromptSuffix() string
		StartProject(workingDir string, port int) (func() error, error)
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

// NewJava initializes the Java language implementation.
func NewJava() Language {
	return java.New()
}

// NewPython initializes the Python language implementation.
func NewPython() Language {
	return python.New()
}
