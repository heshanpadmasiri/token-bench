.PHONY: all run bench clean

GO_SOURCES := $(shell find . -name '*.go' -type f)

RUNS ?= 3
LANGUAGES ?= ballerina go java python
HARNESS ?= claude
SCENARIO ?=
TARGET_DIR ?=
PHOENIX_ENDPOINT ?=

BENCH_PHOENIX_ENDPOINT := http://localhost:6006/
BENCH_PROJECT_SKILLS := $(abspath $(HOME)/Projects/skills/skills)
BENCH_TEST_SKILLS := $(abspath test-skills)

all: token-bench token-bench-report

token-bench: go.mod $(GO_SOURCES)
	go build -o $@ .

token-bench-report: go.mod $(GO_SOURCES)
	go build -o $@ ./cmd/token-bench-report

run: token-bench
	@test -n "$(SCENARIO)" || { echo "SCENARIO is required (for example, SCENARIO=content-based-router)" >&2; exit 2; }
	@test -n "$(TARGET_DIR)" || { echo "TARGET_DIR is required (for example, TARGET_DIR=./results)" >&2; exit 2; }
	@set -e; for language in $(LANGUAGES); do \
		./token-bench "$(SCENARIO)" "$$language" "$(HARNESS)" --n-runs="$(RUNS)" --target="$(TARGET_DIR)" $(if $(strip $(PHOENIX_ENDPOINT)),--phoenix-endpoint="$(PHOENIX_ENDPOINT)"); \
	done

bench: token-bench
	@test -n "$(SCENARIO)" || { echo "SCENARIO is required (for example, SCENARIO=content-based-router)" >&2; exit 2; }
	@test -n "$(TARGET_DIR)" || { echo "TARGET_DIR is required (for example, TARGET_DIR=./results)" >&2; exit 2; }
	@set -e; \
	completed_dir="$(TARGET_DIR)/.token-bench-completed/$(SCENARIO)/$(HARNESS)"; \
	mkdir -p "$$completed_dir"; \
	run_combo() { \
		language="$$1"; skills="$$2"; \
		if [ -n "$$skills" ]; then \
			skills_key="$$(basename "$$skills").$$(printf '%s' "$$skills" | cksum | awk '{print $$1}')"; \
			marker="$$completed_dir/$$language.$$skills_key.done"; \
		else \
			marker="$$completed_dir/$$language.no-skills.done"; \
		fi; \
		if [ -f "$$marker" ]; then \
			echo "Skipping completed benchmark: $$language / $${skills:-no skills} / $(HARNESS)"; \
			return; \
		fi; \
		if [ -n "$$skills" ]; then \
			./token-bench "$(SCENARIO)" "$$language" "$(HARNESS)" --n-runs="$(RUNS)" --target="$(TARGET_DIR)" --skills="$$skills" --phoenix-endpoint="$(BENCH_PHOENIX_ENDPOINT)"; \
		else \
			./token-bench "$(SCENARIO)" "$$language" "$(HARNESS)" --n-runs="$(RUNS)" --target="$(TARGET_DIR)" --phoenix-endpoint="$(BENCH_PHOENIX_ENDPOINT)"; \
		fi; \
		touch "$$marker"; \
	}; \
	for language in $(LANGUAGES); do \
		run_combo "$$language" ""; \
		if [ "$$language" = "ballerina" ]; then \
			run_combo "$$language" "$(BENCH_PROJECT_SKILLS)"; \
			run_combo "$$language" "$(BENCH_TEST_SKILLS)"; \
		fi; \
	done

clean:
	rm -f token-bench token-bench-report
