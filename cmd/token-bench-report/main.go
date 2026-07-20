package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"token-bench/report"
)

const usage = `usage: token-bench-report [--output=report.html] <path> [<path> ...]

Recursively discovers result.json files under each path and generates an HTML report.
`

type options struct {
	output string
	paths  []string
}

func main() {
	configuration, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err := generateFile(configuration.paths, configuration.output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("HTML report:", configuration.output)
}

func parseOptions(arguments []string) (options, error) {
	parsed := options{output: "report.html"}
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		switch {
		case argument == "-h" || argument == "--help":
			fmt.Print(usage)
			os.Exit(0)
		case argument == "--output":
			index++
			if index == len(arguments) || arguments[index] == "" {
				return options{}, errors.New("--output requires a value")
			}
			parsed.output = arguments[index]
		case strings.HasPrefix(argument, "--output="):
			parsed.output = strings.TrimPrefix(argument, "--output=")
			if parsed.output == "" {
				return options{}, errors.New("--output requires a value")
			}
		case strings.HasPrefix(argument, "-"):
			return options{}, fmt.Errorf("unknown option %q", argument)
		default:
			parsed.paths = append(parsed.paths, argument)
		}
	}
	if len(parsed.paths) == 0 {
		return options{}, errors.New("at least one result path is required")
	}
	return parsed, nil
}

func generateFile(paths []string, outputPath string) error {
	absolute, err := filepath.Abs(outputPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(absolute), ".token-bench-report-*.html")
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := report.Generate(paths, temporary); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close report: %w", err)
	}
	if err := os.Rename(temporaryPath, absolute); err != nil {
		return fmt.Errorf("save report: %w", err)
	}
	return nil
}
