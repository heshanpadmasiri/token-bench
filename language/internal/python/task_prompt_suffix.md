## Python project requirements

Use `uv` for dependency management. Keep `main.py` in the project root as the
application entry point and declare all third-party dependencies in
`pyproject.toml`; do not rely on globally installed packages.

The benchmark will start the project from its root using:

```sh
uv run python main.py
```

This command must start all required listeners and remain running until
terminated.
