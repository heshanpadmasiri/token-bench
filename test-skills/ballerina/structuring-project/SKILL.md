---
name: structuring-project
description: Use this when you need to decide on how to structure a project
---

Split your code across 3 files (skip if empty)
1. services.bal
2. config.bal
3. main.bal
4. functions.bal

- Define all your services in services.bal.
- All the configurable variables define them in config.bal
    ```ballerina
    // When you have a very good default value for variable
    configurable string dbHost = "localhost";

    // Use this when you need need to always give this at runtime
    configurable string password = ?;
    ```
- Define all your utility functions in functions.bal
- Define your main function (if needed) in main.bal
  - Use this to initialize module level variables
    ```ballerina
    public function main() returns error? {

    }
    ```
    - For simply variable initialization you can do it inline
  - Note that main function is always executed before the service start
- Also define your module level non configurable variables in main.bal
