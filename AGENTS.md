## Code organization
- For each task/harness/language implementation implement it as a separate internal package and have the corresponding public package expose a single public function to initialize the struct. 

## Harness
- Run each harness as an isolated process in the task's target directory and use the harness's native programmatic transport when available.
  - Run Claude Code as a direct `-p` process with streaming JSON input and output; keep its event stream internal.
  - Run Pi as a direct `--mode rpc` process with JSONL over stdin and stdout; keep its event stream internal.
  - Harnesses without a programmatic transport may use a separate tmux pane and direct keystrokes so the user can observe them.
- Tricky part is getting feedback task is completed.
  - See if the harness has some sort of RPC/websocket/http server implementation that can be used by the benchmark tool for this.  
    - Clearly state all the possible such options to user for a given harness and ask for feedback

## Task
- Start each task in an empty working directory. The shared prompt must tell the agent the language, project initialization command, fixed application port, and every required HTTP endpoint; the agent creates the complete server project.
- Trigger the integration by sending a message to the fixed application port and validate the task by the output.
- Each task can have fixed backend initializations such as mock servers; initialize these with the task's Setup method.
- Once agent tells you it have finished the task validate by sending a set of fixed requests and comparing output. If any of them fail give feedback back to the agent.

## Code guidelines
- Prefer using templates for manual string manipulations.
- For each prompt/generated file have a dedicated prompt file instead instead of inlining it in the code
- Each component (task, language, harness) should be independent from each other.
