// Package report discovers benchmark results and renders an HTML comparison report.
package report

import (
	_ "embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"

	"token-bench/result"
)

//go:embed report.html.tmpl
var reportTemplate string

type discoveredResult struct {
	path  string
	value result.Result
}

type reportData struct {
	Harness    string
	SkillsDirs []string
	TotalRuns  int
	FailedRuns int
	Tasks      []taskData
}

type taskData struct {
	Name        string
	TotalRuns   int
	FailedRuns  int
	NoSuccesses bool
	Languages   []languageData
	Charts      []chartData
}

type languageData struct {
	Name           string
	SkillsDir      string
	Runs           int
	Successful     int
	Failed         int
	PassRate       float64
	AvgCorrections float64
	AvgInput       float64
	AvgOutput      float64
	AvgTotal       float64
	inputs         []float64
	outputs        []float64
	totals         []float64
}

type chartData struct {
	Name       string
	Height     int
	AxisY      int
	AxisTickY  int
	AxisLabelY int
	Ticks      []tickData
	Boxes      []boxData
}

type tickData struct {
	X     float64
	Value float64
}

type boxData struct {
	Language        string
	SkillsDir       string
	Available       bool
	Y               int
	MinimumX        float64
	Q1X             float64
	MedianX         float64
	Q3X             float64
	BoxWidth        float64
	MaximumX        float64
	DeviationStartX float64
	DeviationEndX   float64
	MeanX           float64
	Median          float64
	Deviation       float64
}

// Generate recursively discovers result.json files below paths and writes HTML.
func Generate(paths []string, output io.Writer) error {
	if len(paths) == 0 {
		return errors.New("at least one result path is required")
	}
	discovered, err := discover(paths)
	if err != nil {
		return err
	}
	data, err := aggregate(discovered)
	if err != nil {
		return err
	}
	parsed, err := template.New("report").Parse(reportTemplate)
	if err != nil {
		return fmt.Errorf("parse report template: %w", err)
	}
	if err := parsed.Execute(output, data); err != nil {
		return fmt.Errorf("render report: %w", err)
	}
	return nil
}

func discover(paths []string) ([]discoveredResult, error) {
	seen := make(map[string]struct{})
	var files []string
	add := func(path string) error {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		absolute = filepath.Clean(absolute)
		if _, exists := seen[absolute]; exists {
			return nil
		}
		seen[absolute] = struct{}{}
		files = append(files, absolute)
		return nil
	}
	for _, root := range paths {
		info, err := os.Stat(root)
		if err != nil {
			return nil, fmt.Errorf("inspect %s: %w", root, err)
		}
		if !info.IsDir() {
			if err := add(root); err != nil {
				return nil, err
			}
			continue
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !entry.IsDir() && entry.Name() == "result.json" {
				return add(path)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("search %s: %w", root, err)
		}
	}
	if len(files) == 0 {
		return nil, errors.New("no result.json files found")
	}
	sort.Strings(files)
	values := make([]discoveredResult, 0, len(files))
	for _, path := range files {
		value, err := result.Read(path)
		if err != nil {
			return nil, fmt.Errorf("read result %s: %w", path, err)
		}
		values = append(values, discoveredResult{path: path, value: value})
	}
	return values, nil
}

type variantKey struct {
	language  string
	skillsDir string
}

func aggregate(discovered []discoveredResult) (reportData, error) {
	harnesses := make(map[string]struct{})
	skillsDirs := make(map[string]struct{})
	byTask := make(map[string]map[variantKey][]result.Result)
	data := reportData{TotalRuns: len(discovered)}
	for _, item := range discovered {
		current := item.value
		harnesses[current.Harness] = struct{}{}
		skillsDirs[current.SkillsDir] = struct{}{}
		if !current.Passed {
			data.FailedRuns++
		}
		if byTask[current.Task] == nil {
			byTask[current.Task] = make(map[variantKey][]result.Result)
		}
		key := variantKey{language: current.Language, skillsDir: current.SkillsDir}
		byTask[current.Task][key] = append(byTask[current.Task][key], current)
	}
	if len(harnesses) != 1 {
		names := make([]string, 0, len(harnesses))
		for name := range harnesses {
			names = append(names, name)
		}
		sort.Strings(names)
		return reportData{}, fmt.Errorf("results contain multiple harnesses %q; exactly one is required", names)
	}
	for harness := range harnesses {
		data.Harness = harness
	}
	for _, skillsDir := range sortedKeys(skillsDirs) {
		data.SkillsDirs = append(data.SkillsDirs, skillsLabel(skillsDir))
	}
	taskNames := sortedKeys(byTask)
	for _, taskName := range taskNames {
		task := taskData{Name: taskName}
		for _, key := range sortedVariantKeys(byTask[taskName]) {
			runs := byTask[taskName][key]
			language := summarizeLanguage(key, runs)
			task.Languages = append(task.Languages, language)
			task.TotalRuns += language.Runs
			task.FailedRuns += language.Failed
		}
		task.NoSuccesses = task.TotalRuns == task.FailedRuns
		task.Charts = []chartData{
			makeChart("Total tokens", task.Languages, func(value languageData) []float64 { return value.totals }),
			makeChart("Input tokens", task.Languages, func(value languageData) []float64 { return value.inputs }),
			makeChart("Output tokens", task.Languages, func(value languageData) []float64 { return value.outputs }),
		}
		data.Tasks = append(data.Tasks, task)
	}
	return data, nil
}

func summarizeLanguage(key variantKey, runs []result.Result) languageData {
	value := languageData{Name: key.language, SkillsDir: skillsLabel(key.skillsDir), Runs: len(runs)}
	for _, run := range runs {
		if !run.Passed {
			value.Failed++
			continue
		}
		value.Successful++
		value.AvgCorrections += float64(run.Corrections)
		value.AvgInput += float64(run.Tokens.Input)
		value.AvgOutput += float64(run.Tokens.Output)
		value.AvgTotal += float64(run.Tokens.Total)
		value.inputs = append(value.inputs, float64(run.Tokens.Input))
		value.outputs = append(value.outputs, float64(run.Tokens.Output))
		value.totals = append(value.totals, float64(run.Tokens.Total))
	}
	if value.Runs > 0 {
		value.PassRate = float64(value.Successful) / float64(value.Runs) * 100
	}
	if value.Successful > 0 {
		count := float64(value.Successful)
		value.AvgCorrections /= count
		value.AvgInput /= count
		value.AvgOutput /= count
		value.AvgTotal /= count
	}
	return value
}

func makeChart(name string, languages []languageData, metric func(languageData) []float64) chartData {
	const plotStart = 500.0
	const plotWidth = 650.0
	chart := chartData{
		Name:       name,
		Height:     75 + len(languages)*72,
		AxisY:      35 + len(languages)*72,
		AxisTickY:  42 + len(languages)*72,
		AxisLabelY: 60 + len(languages)*72,
	}
	var maximum float64
	for _, language := range languages {
		for _, value := range metric(language) {
			maximum = max(maximum, value)
		}
	}
	if maximum == 0 {
		maximum = 1
	}
	scale := func(value float64) float64 { return plotStart + value/maximum*plotWidth }
	for index := 0; index <= 4; index++ {
		value := maximum * float64(index) / 4
		chart.Ticks = append(chart.Ticks, tickData{X: scale(value), Value: value})
	}
	for index, language := range languages {
		values := append([]float64(nil), metric(language)...)
		sort.Float64s(values)
		minimum, q1, med, q3, maximumValue, mean, deviation := distribution(values)
		q1X, q3X := scale(q1), scale(q3)
		deviationStartX := scale(max(0, mean-deviation))
		deviationEndX := scale(min(maximum, mean+deviation))
		chart.Boxes = append(chart.Boxes, boxData{
			Language:        language.Name,
			SkillsDir:       language.SkillsDir,
			Available:       len(values) > 0,
			Y:               30 + index*72,
			MinimumX:        scale(minimum),
			Q1X:             q1X,
			MedianX:         scale(med),
			Q3X:             q3X,
			BoxWidth:        q3X - q1X,
			MaximumX:        scale(maximumValue),
			DeviationStartX: deviationStartX,
			DeviationEndX:   deviationEndX,
			MeanX:           scale(mean),
			Median:          med,
			Deviation:       deviation,
		})
	}
	return chart
}

func distribution(ordered []float64) (minimum, q1, median, q3, maximum, mean, deviation float64) {
	if len(ordered) == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}
	minimum, maximum = ordered[0], ordered[len(ordered)-1]
	q1, median, q3 = quantile(ordered, 0.25), quantile(ordered, 0.5), quantile(ordered, 0.75)
	for _, value := range ordered {
		mean += value
	}
	mean /= float64(len(ordered))
	for _, value := range ordered {
		deviation += (value - mean) * (value - mean)
	}
	deviation = math.Sqrt(deviation / float64(len(ordered)))
	return minimum, q1, median, q3, maximum, mean, deviation
}

func quantile(ordered []float64, percentile float64) float64 {
	position := percentile * float64(len(ordered)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return ordered[lower]
	}
	weight := position - float64(lower)
	return ordered[lower]*(1-weight) + ordered[upper]*weight
}

func skillsLabel(path string) string {
	if path == "" {
		return "No skills"
	}
	return path
}

func sortedVariantKeys(values map[variantKey][]result.Result) []variantKey {
	keys := make([]variantKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].language == keys[j].language {
			return keys[i].skillsDir < keys[j].skillsDir
		}
		return keys[i].language < keys[j].language
	})
	return keys
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
