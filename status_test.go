package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func TestStatusModelShowsLatestMultilineMessage(t *testing.T) {
	model := newStatusModel()
	updated, _ := model.Update(tea.WindowSizeMsg{Width: 60, Height: 24})
	model = updated.(statusModel)
	updated, _ = model.Update(statusMessage("Pi\n\nUsing bash\ngo test ./..."))
	model = updated.(statusModel)
	view := model.View()
	for _, expected := range []string{"Agent activity", "Pi", "Using bash", "go test ./..."} {
		if !strings.Contains(view, expected) {
			t.Fatalf("status view does not contain %q:\n%s", expected, view)
		}
	}
}

func TestStatusModelReplacesActivityWithFinalResults(t *testing.T) {
	model := newStatusModel()
	updated, command := model.Update(finalResultsMessage("RUN  PASS\n1    true"))
	model = updated.(statusModel)
	view := model.View()
	if !strings.Contains(view, "Benchmark results") || !strings.Contains(view, "1    true") || strings.Contains(view, "Agent activity") {
		t.Fatalf("unexpected final view:\n%s", view)
	}
	if command == nil {
		t.Fatal("final results did not quit the TUI")
	}
}

func TestStatusDisplayFinishesWithResults(t *testing.T) {
	updates := make(chan string, 1)
	var output bytes.Buffer
	display := startStatusDisplay(updates, &output)
	updates <- "Claude\n\nReading main.go"
	done := make(chan error, 1)
	go func() { done <- display.Finish("RUN  PASS\n1    true") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("status display did not stop")
	}
	if !strings.Contains(output.String(), "Benchmark results") || !strings.Contains(output.String(), "1    true") {
		t.Fatalf("final results were not rendered:\n%s", output.String())
	}
}
