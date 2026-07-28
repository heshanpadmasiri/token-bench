# token-bench

`token-bench` benchmarks coding agents by asking them to build integrations from scratch. For each run it creates an isolated project directory, starts the agent harness, checks the generated service, provides corrective feedback when needed, runs hidden validation, and records token usage and results.

## Supported configurations

| Dimension | Values |
| --- | --- |
| Scenarios | `claim-check`, `content-based-router`, `content-enricher`, `dead-letter-channel`, `multi-consumer`, `scatter-gather` |
| Languages | `ballerina`, `go`, `java`, `python` |
| Harnesses | `claude`, `pi` |

## Setup

### Requirements

- Go 1.25.3 or newer
- At least one configured and authenticated coding harness on `PATH`:
  - `claude` for the Claude Code harness
  - `pi` for the Pi harness
- The toolchain for every target language you intend to benchmark:
  - Ballerina: `bal`
  - Go: `go`
  - Java: JDK 21 and `mvn`
  - Python: `uv` with Python 3.12 available
- Docker with a running daemon for scenarios backed by Redis, PostgreSQL, RabbitMQ, or Kafka (`claim-check`, `content-enricher`, `dead-letter-channel`, and `multi-consumer`)
- Optional: a local [Arize Phoenix](https://phoenix.arize.com/) instance for tracing

Check that the required commands are available, for example:

```sh
go version
claude --version   # or: pi --version
docker info
```

Download dependencies, run the tests, and build both executables:

```sh
go mod download
go test ./...
make all
```

This creates:

- `./token-bench` — benchmark runner with a terminal UI
- `./token-bench-report` — HTML report generator

Remove the built executables with:

```sh
make clean
```

## Run the harness

Each benchmark needs a scenario, one or more target languages, a harness, and a directory in which to retain generated projects and result files.

### Make commands

Run a scenario across the default languages (`ballerina go java python`) using Claude Code:

```sh
make run \
  SCENARIO=content-based-router \
  TARGET_DIR=./results
```

Customize the run with Make variables:

```sh
make run \
  SCENARIO=scatter-gather \
  TARGET_DIR=./results \
  LANGUAGES="go python" \
  HARNESS=pi \
  RUNS=1
```

The available variables are:

| Variable | Default | Description |
| --- | --- | --- |
| `SCENARIO` | required | Scenario name from the supported list above |
| `TARGET_DIR` | required | Parent directory for generated projects and results |
| `LANGUAGES` | `ballerina go java python` | Space-separated target languages |
| `HARNESS` | `claude` | `claude` or `pi` |
| `RUNS` | `3` | Number of runs per language |
| `PHOENIX_ENDPOINT` | empty | Optional Phoenix endpoint, such as `http://localhost:6006/` |

For example, enable tracing for a Claude benchmark with:

```sh
make run \
  SCENARIO=content-enricher \
  TARGET_DIR=./results \
  LANGUAGES=go \
  PHOENIX_ENDPOINT=http://localhost:6006/
```

> Phoenix tracing is currently supported by the Claude harness only.

For the full benchmark matrix, use `make bench`:

```sh
make bench \
  SCENARIO=content-based-router \
  TARGET_DIR=./results \
  HARNESS=claude \
  RUNS=3
```

`make bench` runs every selected language without skills, then runs Ballerina again with the project and test skill sets. It expects Phoenix at `http://localhost:6006/` and stores completion markers under `TARGET_DIR/.token-bench-completed`, allowing an interrupted matrix to resume without repeating completed combinations. Its skill locations default to `~/Projects/skills/skills` and `./test-skills`; override them when necessary:

```sh
make bench \
  SCENARIO=content-based-router \
  TARGET_DIR=./results \
  BENCH_PROJECT_SKILLS=/path/to/project-skills \
  BENCH_TEST_SKILLS="$PWD/test-skills"
```

### Direct CLI usage

Build the runner, then invoke it directly:

```sh
make token-bench
./token-bench <task> <language> <harness> [options]
```

A single Go/Pi run looks like this:

```sh
./token-bench content-based-router go pi \
  --n-runs=1 \
  --target=./results
```

The terminal UI displays the agent's current activity while the benchmark is running and replaces it with a result table when all runs finish.

Options:

| Option | Default | Description |
| --- | --- | --- |
| `--n-runs=N` | `1` | Number of independent runs |
| `--target=DIR` | temporary directory | Directory in which generated projects and results are retained |
| `--skills=DIR` | none | Skills directory supplied to the harness |
| `--phoenix-endpoint=URL` | disabled | Local Phoenix OTLP/HTTP endpoint (Claude only) |

Options also accept a separate value, such as `--target ./results`. Claude loads `--skills` as a temporary plugin; its global configuration may still be discovered. The Pi harness adds the supplied skill directory to Pi's startup configuration.

If `--target` is omitted, the runner creates a temporary directory and prints its result path at completion. To inspect generated code and logs reliably, set an explicit target directory.

## Results and reports

Every run gets its own directory beneath the target and contains a `result.json` plus harness stream and stderr artifacts. The final TUI table reports validation status, corrective feedback count, token usage, and any error.

Build an HTML report by recursively collecting results from one or more paths:

```sh
make token-bench-report
./token-bench-report --output=report.html ./results
```

Multiple result roots may be supplied:

```sh
./token-bench-report --output=report.html ./results/baseline ./results/with-skills
```

## Adding new scenarios
> In most case you can ask your agent to add the scenario by using the `add-test-scenario`. Just validating the generated test should be enough

- Each test scenario must conform to `Task` and `TaskHandle` interfaces defined in `task.go`
- `Task` defines resources you want to get allocated from the benchmark harness as well as metadata it use to track the scenario
- It will allocated the those resources and pass them in when creating the `TaskHandle`
- Benchmark tool first call `Setup()` method in the `TaskHandle`. You should use this to setup any back-end services such as databases, docker containers etc. You return a tear down function that harness will call after completing a given run
- `Prompt` should then build and return the task specific prompt we provide to the agent to start the turn.
- Once agent has indicated that task is done harness will call `feedback`, while the agent generated code is running. You need to validate agent's implementation using a "visible" (there is a these could be exposed to agent depending on your feedback so it may try to hard code responses) set of test cases and provide feedback that is passed back to the agent.
- If you provide no feedback, we then move to `validation` (you should use a different set of values for this) where you should indicate to harness that task was implemented successfully or not
