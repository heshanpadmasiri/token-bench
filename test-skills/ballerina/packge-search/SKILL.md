---
name: package search
description: Use this when you need to find information about available package or documentation on specific package
---
- IMPORTANT: Don't directly modify Dependencies.toml or Ballerina.toml in order to add dependencies
Use `central-search` for finding information about available packages and documentation about them. Use `central-search llm` command to get a detailed description on how to use it. After identifying packages you need to use simply import those packages in ballerina source. `bal build` command will automatically resolve the latest compatible versions of packages taking into account all your imports.

- When importing database connectors make sure to import the driver as well
  ```ballerina
  import ballerinax/postgresql;
  import ballerinax/postgresql.driver as _;
  ```
