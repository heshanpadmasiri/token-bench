package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode"

	"token-bench/harness"
	"token-bench/language"
	benchresult "token-bench/result"
	"token-bench/task"
)

// TODO: add optional harness flags (system prompt, model, thinking)
const usage = `usage: token-bench <task> <language> <harness> [--n-runs=N] [--target=DIR] [--skills=DIR]

Implementations:
  task:      content-based-router, scatter-gather
  language:  ballerina, go, java
  harness:   claude, pi

Notes:
  claude loads --skills as a temporary plugin; global Claude configuration may still be discovered.
`

type options struct {
	task      string
	language  string
	harness   string
	runs      int
	target    string
	skillsDir string
}

type runResult = benchresult.Result

type tokenSummary struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	Total      float64 `json:"total"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
}

type report struct {
	Task      string       `json:"task"`
	Language  string       `json:"language"`
	Harness   string       `json:"harness"`
	SkillsDir string       `json:"skillsDir,omitempty"`
	Runs      []runResult  `json:"runs"`
	Mean      tokenSummary `json:"mean"`
	Median    tokenSummary `json:"median"`
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
	configuration.skillsDir, err = normalizeSkillsDir(configuration.skillsDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	harnessConstructor, err := selectHarness(configuration.harness, harness.Config{SkillsDir: configuration.skillsDir})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	target, err := prepareTarget(configuration.target)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result := report{Task: taskDefinition.Name, Language: configuration.language, Harness: configuration.harness, SkillsDir: configuration.skillsDir}
	for run := 1; run <= configuration.runs; run++ {
		directory, err := createRunDirectory(target, configuration.language, taskDefinition.Name, configuration.harness)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		current := runResult{
			SchemaVersion: benchresult.SchemaVersion,
			Task:          taskDefinition.Name,
			Language:      configuration.language,
			Harness:       configuration.harness,
			SkillsDir:     configuration.skillsDir,
			Run:           run,
			RequestedRuns: configuration.runs,
		}
		current = executeRun(current, directory, taskDefinition, languageImplementation, harnessConstructor)
		result.Runs = append(result.Runs, current)
		if err := benchresult.Write(filepath.Join(directory, "result.json"), current); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	result.Mean, result.Median = summarize(result.Runs)
	if err := printReport(result, target); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, current := range result.Runs {
		if !current.Passed {
			os.Exit(1)
		}
	}
}

func executeRun(current runResult, directory string, definition task.Task, implementation language.Language, newHarness func() harness.Harness) (result runResult) {
	result = current
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
	agent := newHarness()
	if err := agent.Start(directory); err != nil {
		result.Error = err.Error()
		return result
	}
	defer func() {
		if count, err := agent.TokenCount(); err == nil {
			result.Tokens = benchresult.TokenCount{
				Input: count.Input, Output: count.Output, Total: count.Total,
				CacheRead: count.CacheRead, CacheWrite: count.CacheWrite,
			}
		} else {
			result.Error = joinErrors(result.Error, err.Error())
		}
		if err := agent.End(); err != nil {
			result.Error = joinErrors(result.Error, err.Error())
		}
	}()

	requirements, err := handle.Prompt()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	prompt, err := task.RenderPrompt(task.PromptConfig{
		Language:          implementation.Name(),
		InitializeCommand: implementation.InitializeCommand(),
		Port:              ports[0],
		Endpoints:         definition.Endpoints,
		Requirements:      requirements,
	})
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if _, err := agent.SendMessage(prompt); err != nil {
		result.Error = err.Error()
		return result
	}

	for {
		stopProject, startErr := implementation.StartProject(directory, ports[0])
		feedback := ""
		if startErr != nil {
			feedback = fmt.Sprintf("The application failed to start. Exact diagnosis:\n%s", startErr)
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
	switch name {
	case "content-based-router":
		return task.NewContentBasedRouter(), nil
	case "scatter-gather":
		return task.NewScatterGather(), nil
	default:
		return task.Task{}, fmt.Errorf("unknown task %q", name)
	}
}

func selectLanguage(name string) (language.Language, error) {
	switch name {
	case "ballerina":
		return language.NewBallerina(), nil
	case "go":
		return language.NewGo(), nil
	case "java":
		return language.NewJava(), nil
	default:
		return nil, fmt.Errorf("unknown language %q", name)
	}
}

func selectHarness(name string, config harness.Config) (func() harness.Harness, error) {
	switch name {
	case "claude":
		return func() harness.Harness { return harness.NewClaude(config) }, nil
	case "pi":
		return func() harness.Harness { return harness.NewPi(config) }, nil
	default:
		return nil, fmt.Errorf("unknown harness %q", name)
	}
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
		case strings.HasPrefix(argument, "--skills="):
			if parsed.skillsDir != "" {
				return options{}, errors.New("--skills may only be specified once")
			}
			parsed.skillsDir = strings.TrimPrefix(argument, "--skills=")
			if parsed.skillsDir == "" {
				return options{}, errors.New("--skills requires a value")
			}
		case argument == "--skills":
			if parsed.skillsDir != "" {
				return options{}, errors.New("--skills may only be specified once")
			}
			index++
			if index == len(arguments) || arguments[index] == "" {
				return options{}, errors.New("--skills requires a value")
			}
			parsed.skillsDir = arguments[index]
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

func normalizeSkillsDir(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("normalize skills directory: %w", err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf("inspect skills directory: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("skills path %q is not a directory", path)
	}
	return filepath.Clean(absolute), nil
}

func normalizeRunName(value string) string {
	return strings.Map(func(character rune) rune {
		if unicode.IsSpace(character) {
			return '_'
		}
		return character
	}, value)
}

func runDirectoryPrefix(language, task, harness string) string {
	return fmt.Sprintf("%s-%s-%s-", normalizeRunName(language), normalizeRunName(task), normalizeRunName(harness))
}

func createRunDirectory(target, language, task, harness string) (string, error) {
	return os.MkdirTemp(target, runDirectoryPrefix(language, task, harness)+"*")
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

func printReport(result report, target string) error {
	writer := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	skills := result.SkillsDir
	if skills == "" {
		skills = "No skills"
	}
	if _, err := fmt.Fprintf(writer, "SKILLS\t%s\n", skills); err != nil {
		return err
	}
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
	pattern := runDirectoryPrefix(result.Language, result.Task, result.Harness) + "*"
	_, err := fmt.Println("JSON results:", filepath.Join(target, pattern, "result.json"))
	return err
}

func joinErrors(current, additional string) string {
	if current == "" {
		return additional
	}
	return current + "; " + additional
}
