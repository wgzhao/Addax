AGENTS.md
=========

Purpose
-------
This file is written for AI coding agents that are assigned to work in the Addax repository. It contains the minimal, concrete, and discoverable knowledge needed to be productive quickly: the high-level architecture, key developer workflows (build/test/debug), codebase-specific conventions, integration points, and a short troubleshooting example (protobuf runtime issue found in `plugin/reader/dorisreader`).

High-level architecture (big picture)
------------------------------------
- Monorepo built with Maven. Parent POM at project root (`pom.xml`) aggregates modules (core, plugin, server, etc.).
- Major components:
  - `core/` : Addax core framework and runtime (Engine, JobContainer, common utils).
  - `plugin/` : Source for reader/writer plugins. Each plugin is a separate Maven module (e.g., `plugin/reader/dorisreader`). Plugins implement `com.wgzhao.addax.core.spi.Reader`/Writer interfaces.
  - `docs/`, `overrides/`, `server/` : documentation, server-side web templates and pages.
- Dataflow: a Job is created by the `Engine` which uses `JobContainer` → `Reader.Job` splits into slices → `Reader.Task` runs and uses `RecordSender` to send records into downstream Writer. See `com.wgzhao.addax.core.job.JobContainer` and `com.wgzhao.addax.core.Engine` for orchestration.

Key developer workflows
-----------------------
- Build whole repository:

  mvn -T1C -DskipTests package

  (Use the parent `pom.xml` in repo root. Some modules produce jars under `target/`.)

- Build a single plugin module (example: dorisreader):

  cd plugin/reader/dorisreader
  mvn -DskipTests package

- Run docs build: `./build-docs.sh` (see `mkdocs.yml` and `docs/` folder)
- Running the engine locally: the runtime entrypoint is `com.wgzhao.addax.core.Engine` (check `core/src/main/java`). Use `java -jar` on an assembled `addax-core` jar or run with `mvn -pl core -am exec:java` adjusted to your environment.

Project-specific conventions and patterns
---------------------------------------
- Plugins follow a standard pattern: outer class extends `Reader` or `Writer` with static nested `Job` and `Task` classes. Example: `plugin/reader/dorisreader/src/main/java/com/wgzhao/addax/plugin/reader/dorisreader/DorisReader.java`.
- Plugins often delegate heavy lifting to shared helpers in `addax-rdbms` (e.g., `CommonRdbmsReader.Job/Task`). Prefer reusing these common classes for JDBC/SQL-based readers.
- Configuration is handled via `com.wgzhao.addax.core.util.Configuration` and constants/keys in `com.wgzhao.addax.core.base.Key`. Look up how values are read/set in existing plugins before adding new config keys.
- Error handling: the code raises `AddaxException.asAddaxException(ErrorCode.<...>, "message")` for input validation and fatal plugin errors. Follow the same pattern.

Integration points and notable external dependencies
---------------------------------------------------
- JDBC-based readers/writers use `BasicDataSource` (commons-dbcp2) and utility `com.wgzhao.addax.rdbms.util.DBUtil`.
- Some plugins (e.g., `dorisreader`) depend on Apache Arrow Flight and the Arrow JDBC driver: `org.apache.arrow:flight-sql-jdbc-core`. Arrow transitively depends on `protobuf` runtime. Be careful: many protobuf versions exist across the JVM classpath; prefer adding an explicit `com.google.protobuf:protobuf-java` dependency in the plugin or managing it centrally in the parent POM.

Troubleshooting example: NoClassDefFoundError for protobuf RuntimeVersion
---------------------------------------------------------------------
- Symptom: plugin fails at runtime with

  java.lang.NoClassDefFoundError: com/google/protobuf/RuntimeVersion$RuntimeDomain

  stack shows Arrow Flight classes (Flight$Ticket) failing to initialize.

- Root cause: Arrow Flight requires a protobuf runtime class that isn't on the runtime classpath or a mismatched protobuf version is present. The missing nested class `RuntimeVersion$RuntimeDomain` indicates protobuf runtime incompatible or absent.
- Fix used in this repo (concrete change): add explicit dependency in `plugin/reader/dorisreader/pom.xml`:

  <dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>3.21.12</version>
  </dependency>

  If your environment manages protobuf at the parent level, prefer adding the version into the parent POM `dependencyManagement` so all modules use a consistent protobuf runtime.

Where to look next when similar errors appear
--------------------------------------------
- Inspect the dependency tree for conflicting protobuf versions:

  mvn dependency:tree -Dincludes=com.google.protobuf:protobuf-java

- If you find multiple versions, prefer aligning them in parent `dependencyManagement` or exclude older versions from transitive dependencies.
- Check the runtime classpath used by the process starting `Engine`. When running via IDE or custom launcher, ensure it uses the Maven-built classpath or an assembly jar that includes required dependencies.

Key files and locations to inspect
---------------------------------
- Parent build: `pom.xml` at repo root
- Core runtime: `core/src/main/java/com/wgzhao/addax/core` (Engine, JobContainer)
- RDBMS helpers: `lib/addax-rdbms` and package `com.wgzhao.addax.rdbms` (watch for `DBUtil`, `CommonRdbmsReader`)
- Plugins: `plugin/reader/*` and `plugin/writer/*` (follow plugin template)
- Docs: `docs/` and `docs/en/` for example configurations

If you are an AI agent assigned to modify code
-------------------------------------------
- Keep changes minimal and module-scoped (avoid large parent POM edits without justification).
- When adding runtime dependencies to fix NoClassDefFoundError, add them into the module POM and explain why in the commit message, then propose moving them to `dependencyManagement` if multiple modules need the same fix.

Contact/Owner notes
-------------------
- No explicit owners declared in repository files. Use the commit log and existing PRs to identify maintainers.

Short changelog entry for today's fix
------------------------------------
- Added explicit protobuf-java dependency to `plugin/reader/dorisreader/pom.xml` to avoid NoClassDefFoundError at runtime when using Arrow Flight.

End of AGENTS.md

