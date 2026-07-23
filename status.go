package main

import (
	"io"
	"sync"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	defaultStatusWidth  = 80
	defaultStatusHeight = 8
	maxStatusHeight     = 12
)

type (
	statusMessage       string
	finalResultsMessage string
)

type statusModel struct {
	spinner      spinner.Model
	viewport     viewport.Model
	latest       string
	finalResults string
	done         bool
}

func newStatusModel() statusModel {
	indicator := spinner.New()
	indicator.Spinner = spinner.Dot
	view := viewport.New(defaultStatusWidth, defaultStatusHeight)
	view.MouseWheelEnabled = false
	return statusModel{spinner: indicator, viewport: view}
}

func (m statusModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m statusModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch current := message.(type) {
	case statusMessage:
		m.latest = string(current)
		m.setContent()
		return m, nil
	case finalResultsMessage:
		m.finalResults = string(current)
		m.done = true
		return m, tea.Quit
	case tea.WindowSizeMsg:
		m.viewport.Width = max(1, current.Width)
		m.viewport.Height = max(1, min(maxStatusHeight, current.Height/3))
		m.setContent()
	}
	var commands []tea.Cmd
	var command tea.Cmd
	m.spinner, command = m.spinner.Update(message)
	commands = append(commands, command)
	m.viewport, command = m.viewport.Update(message)
	commands = append(commands, command)
	return m, tea.Batch(commands...)
}

func (m statusModel) View() string {
	if m.done {
		if m.finalResults == "" {
			return ""
		}
		header := lipgloss.NewStyle().Bold(true).Render("Benchmark results")
		return header + "\n\n" + m.finalResults
	}
	header := lipgloss.NewStyle().Bold(true).Render(m.spinner.View() + " Agent activity")
	return header + "\n" + m.viewport.View()
}

func (m *statusModel) setContent() {
	m.viewport.SetContent(lipgloss.NewStyle().Width(m.viewport.Width).Render(m.latest))
	m.viewport.GotoBottom()
}

type statusDisplay struct {
	updates    chan string
	program    *tea.Program
	bridgeDone chan struct{}
	done       chan struct{}
	err        error
	once       sync.Once
}

func startStatusDisplay(updates chan string, output io.Writer) *statusDisplay {
	display := &statusDisplay{updates: updates, bridgeDone: make(chan struct{}), done: make(chan struct{})}
	display.program = tea.NewProgram(newStatusModel(), tea.WithOutput(output), tea.WithInput(nil))
	go func() {
		_, display.err = display.program.Run()
		close(display.done)
	}()
	go func() {
		for message := range updates {
			display.program.Send(statusMessage(message))
		}
		close(display.bridgeDone)
	}()
	return display
}

func (d *statusDisplay) Finish(finalResults string) error {
	d.once.Do(func() {
		close(d.updates)
		<-d.bridgeDone
		d.program.Send(finalResultsMessage(finalResults))
	})
	<-d.done
	return d.err
}

func (d *statusDisplay) Close() error {
	return d.Finish("")
}
