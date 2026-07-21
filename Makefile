.PHONY: all clean

GO_SOURCES := $(shell find . -name '*.go' -type f)

all: token-bench token-bench-report

token-bench: go.mod $(GO_SOURCES)
	go build -o $@ .

token-bench-report: go.mod $(GO_SOURCES)
	go build -o $@ ./cmd/token-bench-report

clean:
	rm -f token-bench token-bench-report
