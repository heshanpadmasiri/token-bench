---
name: add-test-scenario
description: Adds a new token-bench HTTP test scenario (task), including its prompt, mock backends, feedback checks, hidden validation, registration, and tests. Use when creating or extending benchmark scenarios under task/.
---

# Add a Test Scenario

1. Define the contract: scenario name, required endpoints, application port plus backend port count, request/response behavior, failure cases, and what must be preserved or run concurrently.
2. Create `task/internal/<scenario>/` with:
   - `<scenario>.go`: a private `task.TaskHandle` implementation.
   - `prompt.md.tmpl`: task-specific requirements and examples; interpolate allocated backend URLs rather than inlining prompt text in Go.
   - `<scenario>_test.go`: prompt, setup/cleanup, checks, and reference-implementation tests.
3. Implement the lifecycle:
   - `New(ports)` validates all allocated ports.
   - `Setup()` starts benchmark-owned mock backends and returns an idempotent, non-nil cleanup function.
   - `Prompt()` renders the dedicated template.
   - `Feedback()` sends fixed public requests to the generated application and returns precise, actionable diagnostics for the first failure.
   - `Validation()` uses separate hidden cases and returns pass/fail without revealing them.
4. Verify externally observable behavior, not implementation details: status, body, headers, routing/fan-out, backend request count, malformed/schema-invalid input, and no backend calls when input is rejected. Use timeouts and synchronized request recording; cleanly shut down every server.
5. Expose one public constructor in `task/task.go` with `Name`, `Resources.NPorts`, every required `Endpoint`, and `InitFn`.
6. Register the scenario in `selectTask` and the CLI usage list in `main.go`; update selection tests in `main_test.go`.
7. Run `gofmt` on Go files and `go test ./...`.

Keep the shared prompt responsible for language, initialization command, fixed application port, and endpoint list. Keep scenario-specific behavior in its own prompt template and internal package.

> [!IMPORTANT]
> prompt must explicitly say the back end is running and agent can use it for implementation. But **MUST NEVER TRY TO KILL IT**.
