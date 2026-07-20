package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"token-bench/harness"
	"token-bench/language"
	"token-bench/task"
)

// TODO: add optional harness flags (system prompt, skills, model, thinking)
const usage = `usage: token-bench <task> <language> <harness> [--n-runs=N] [--target=DIR]

Implementations:
  task:      content-based-router
  language:  ballerina
  harness:   pi
`

type options struct {
	task     string
	language string
	harness  string
	runs     int
	target   string
}

type runResult struct {
	Run         int                `json:"run"`
	Passed      bool               `json:"passed"`
	Corrections int                `json:"corrections"`
	Tokens      harness.TokenCount `json:"tokens"`
	Error       string             `json:"error,omitempty"`
}

type tokenSummary struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	Total      float64 `json:"total"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
}

type report struct {
	Task     string       `json:"task"`
	Language string       `json:"language"`
	Harness  string       `json:"harness"`
	Runs     []runResult  `json:"runs"`
	Mean     tokenSummary `json:"mean"`
	Median   tokenSummary `json:"median"`
}

func main() {
	configuration, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	taskDefinition, err := selectTask(configuration.task)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	languageImplementation, err := selectLanguage(configuration.language)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	harnessConstructor, err := selectHarness(configuration.harness)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	target, err := prepareTarget(configuration.target)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result := report{Task: taskDefinition.Name, Language: configuration.language, Harness: configuration.harness}
	for run := 1; run <= configuration.runs; run++ {
		directory := filepath.Join(target, fmt.Sprintf("run-%03d", run))
		if err := os.Mkdir(directory, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		result.Runs = append(result.Runs, executeRun(run, directory, taskDefinition, languageImplementation, harnessConstructor))
	}
	result.Mean, result.Median = summarize(result.Runs)
	resultsPath := filepath.Join(target, "results.json")
	if err := writeReport(resultsPath, result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := printReport(result, resultsPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, current := range result.Runs {
		if !current.Passed {
			os.Exit(1)
		}
	}
}

func executeRun(run int, directory string, definition task.Task, implementation language.Language, newHarness func() harness.Harness) (result runResult) {
	result.Run = run
	ports, err := allocatePorts(definition.Resources.NPorts)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	allocated := task.AllocResources{Ports: ports}
	defer allocated.Cleanup()
	handle := definition.InitFn(allocated)
	cleanupTask, err := handle.Setup()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer func() {
		if err := cleanupTask(); err != nil {
			result.Error = joinErrors(result.Error, err.Error())
		}
	}()

	if len(ports) == 0 {
		result.Error = "task allocated no application port"
		return result
	}
	if err := implementation.SetupProject(directory, ports[0]); err != nil {
		result.Error = err.Error()
		return result
	}
	agent := newHarness()
	if err := agent.Start(directory); err != nil {
		result.Error = err.Error()
		return result
	}
	defer func() {
		if count, err := agent.TokenCount(); err == nil {
			result.Tokens = count
		} else {
			result.Error = joinErrors(result.Error, err.Error())
		}
		if err := agent.End(); err != nil {
			result.Error = joinErrors(result.Error, err.Error())
		}
	}()

	prompt, err := handle.Prompt()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if _, err := agent.SendMessage(prompt); err != nil {
		result.Error = err.Error()
		return result
	}

	for {
		stopProject, startErr := implementation.StartProject(directory)
		feedback := ""
		if startErr != nil {
			feedback = fmt.Sprintf("The Ballerina application failed to start. Exact diagnosis:\n%s", startErr)
		} else {
			feedback, err = handle.Feedback()
			if err == nil && feedback == "" {
				result.Passed, err = handle.Validation()
			}
			if stopErr := stopProject(); stopErr != nil {
				err = errors.Join(err, stopErr)
			}
		}
		if err != nil {
			result.Error = err.Error()
			return result
		}
		if result.Passed {
			return result
		}
		if feedback == "" {
			result.Error = "hidden validation failed"
			return result
		}
		if result.Corrections == 3 {
			result.Error = feedback
			return result
		}
		result.Corrections++
		if _, err := agent.SendMessage(feedback); err != nil {
			result.Error = err.Error()
			return result
		}
	}
}

func selectTask(name string) (task.Task, error) {
	if name != "content-based-router" {
		return task.Task{}, fmt.Errorf("unknown task %q", name)
	}
	return task.NewContentBasedRouter(), nil
}

func selectLanguage(name string) (language.Language, error) {
	if name != "ballerina" {
		return nil, fmt.Errorf("unknown language %q", name)
	}
	return language.NewBallerina(), nil
}

func selectHarness(name string) (func() harness.Harness, error) {
	if name != "pi" {
		return nil, fmt.Errorf("unknown harness %q", name)
	}
	return harness.NewPi, nil
}

func parseOptions(arguments []string) (options, error) {
	parsed := options{runs: 1}
	positionals := make([]string, 0, 3)
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		switch {
		case argument == "-h" || argument == "--help":
			fmt.Print(usage)
			os.Exit(0)
		case strings.HasPrefix(argument, "--n-runs="):
			value, err := strconv.Atoi(strings.TrimPrefix(argument, "--n-runs="))
			if err != nil || value < 1 {
				return options{}, errors.New("--n-runs must be a positive integer")
			}
			parsed.runs = value
		case argument == "--n-runs":
			index++
			if index == len(arguments) {
				return options{}, errors.New("--n-runs requires a value")
			}
			value, err := strconv.Atoi(arguments[index])
			if err != nil || value < 1 {
				return options{}, errors.New("--n-runs must be a positive integer")
			}
			parsed.runs = value
		case strings.HasPrefix(argument, "--target="):
			parsed.target = strings.TrimPrefix(argument, "--target=")
		case argument == "--target":
			index++
			if index == len(arguments) {
				return options{}, errors.New("--target requires a value")
			}
			parsed.target = arguments[index]
		case strings.HasPrefix(argument, "-"):
			return options{}, fmt.Errorf("unknown option %q", argument)
		default:
			positionals = append(positionals, argument)
		}
	}
	if len(positionals) != 3 {
		return options{}, errors.New("task, language, and harness are required")
	}
	parsed.task, parsed.language, parsed.harness = positionals[0], positionals[1], positionals[2]
	return parsed, nil
}

func allocatePorts(count int) ([]int, error) {
	ports := make([]int, 0, count)
	seen := make(map[int]struct{}, count)
	for len(ports) < count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("allocate port: %w", err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if err := listener.Close(); err != nil {
			return nil, fmt.Errorf("release port: %w", err)
		}
		if _, duplicate := seen[port]; duplicate {
			continue
		}
		seen[port] = struct{}{}
		ports = append(ports, port)
	}
	return ports, nil
}

func prepareTarget(target string) (string, error) {
	if target == "" {
		created, err := os.MkdirTemp("", "token-bench-")
		if err != nil {
			return "", err
		}
		return created, nil
	}
	absolute, err := filepath.Abs(target)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(absolute, 0o755); err != nil {
		return "", err
	}
	return absolute, nil
}

func summarize(runs []runResult) (tokenSummary, tokenSummary) {
	inputs := make([]float64, 0, len(runs))
	outputs := make([]float64, 0, len(runs))
	totals := make([]float64, 0, len(runs))
	reads := make([]float64, 0, len(runs))
	writes := make([]float64, 0, len(runs))
	for _, run := range runs {
		inputs = append(inputs, float64(run.Tokens.Input))
		outputs = append(outputs, float64(run.Tokens.Output))
		totals = append(totals, float64(run.Tokens.Total))
		reads = append(reads, float64(run.Tokens.CacheRead))
		writes = append(writes, float64(run.Tokens.CacheWrite))
	}
	mean := tokenSummary{mean(inputs), mean(outputs), mean(totals), mean(reads), mean(writes)}
	median := tokenSummary{median(inputs), median(outputs), median(totals), median(reads), median(writes)}
	return mean, median
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total float64
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	middle := len(ordered) / 2
	if len(ordered)%2 == 1 {
		return ordered[middle]
	}
	return (ordered[middle-1] + ordered[middle]) / 2
}

// TODO:this should allow us to accumilate results (With a special --acum flag)
func writeReport(path string, result report) error {
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o644)
}

func printReport(result report, path string) error {
	writer := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "RUN\tPASS\tFIXES\tINPUT\tOUTPUT\tCACHE READ\tCACHE WRITE\tTOTAL\tERROR"); err != nil {
		return err
	}
	for _, run := range result.Runs {
		if _, err := fmt.Fprintf(writer, "%d\t%t\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n", run.Run, run.Passed, run.Corrections, run.Tokens.Input, run.Tokens.Output, run.Tokens.CacheRead, run.Tokens.CacheWrite, run.Tokens.Total, strings.Join(strings.Fields(run.Error), " ")); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(writer, "MEAN\t-\t-\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t-\n", result.Mean.Input, result.Mean.Output, result.Mean.CacheRead, result.Mean.CacheWrite, result.Mean.Total); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "MEDIAN\t-\t-\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t-\n", result.Median.Input, result.Median.Output, result.Median.CacheRead, result.Median.CacheWrite, result.Median.Total); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	_, err := fmt.Println("JSON results:", path)
	return err
}

func joinErrors(current, additional string) string {
	if current == "" {
		return additional
	}
	return current + "; " + additional
}
