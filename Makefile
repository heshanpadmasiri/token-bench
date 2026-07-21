.PHONY: all run clean

GO_SOURCES := $(shell find . -name '*.go' -type f)

RUNS ?= 3
LANGUAGES ?= ballerina go java
HARNESS ?= claude
SCENARIO ?=
TARGET_DIR ?=

all: token-bench token-bench-report

token-bench: go.mod $(GO_SOURCES)
	go build -o $@ .

token-bench-report: go.mod $(GO_SOURCES)
	go build -o $@ ./cmd/token-bench-report

run: token-bench
	@test -n "$(SCENARIO)" || { echo "SCENARIO is required (for example, SCENARIO=content-based-router)" >&2; exit 2; }
	@test -n "$(TARGET_DIR)" || { echo "TARGET_DIR is required (for example, TARGET_DIR=./results)" >&2; exit 2; }
	@set -e; for language in $(LANGUAGES); do \
		./token-bench "$(SCENARIO)" "$$language" "$(HARNESS)" --n-runs="$(RUNS)" --target="$(TARGET_DIR)"; \
	done

clean:
	rm -f token-bench token-bench-report
