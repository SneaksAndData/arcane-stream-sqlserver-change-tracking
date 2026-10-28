# AGENTS.md

Scala 3 (`3.9.0`) plugin for the Arcane data platform: a single streaming app (`com.sneaksanddata.arcane.sql_server_change_tracking.main`)
that captures SQL Server Change Tracking changes and merges them into an Iceberg table. Most logic lives in the
`arcane-framework-scala` dependency; this repo mainly wires up the plugin's DI layers (`src/main/scala/main.scala`),
`MicrosoftSqlServerPluginStreamContext`/`MicrosoftSqlServerSourceSettings`, and integration tests.

## Prerequisites

- `GITHUB_TOKEN` env var (or a `.ghtoken` file in repo root) must be set/exist — it authenticates against the
  GitHub Maven registry for the `arcane-framework-scala` dependency. **Nothing builds without it.**
- Use `mise` + `just`; do not call `sbt`/`docker` directly unless replicating what a `just` recipe does, since they
  set required env/tool versions (JDK Temurin 25, sbt 1.12.9 — see `mise.toml`).

## Commands (via `justfile`)

- `just build` — `sbt assembly`, produces `target/com.sneaksanddata.arcane.sql-server-change-tracking.assembly.jar`.
- `just check` — `sbt scalafmtCheckAll` (run before committing; CI fails the build on formatting drift).
- `just clean` — `sbt clean`.
- `just it` — full integration test run: brings up `docker-compose.yaml` (MinIO/S2, Lakekeeper + Postgres, Trino,
  a real MSSQL server seeded via `integration-tests.sql`), waits for health, then runs `sbt test` with env from
  `integration-tests.env` (loaded by `mise.it.toml`). Tears down compose on exit via `trap`.
- `just stream [--debug]` / `just backfill [--debug]` — runs the assembled JAR locally against `dev.env`
  (copy from `dev.env.example`; the file is required and the recipe aborts if it's missing). `backfill` just sets
  `STREAMCONTEXT__BACKFILL=true` and calls `stream`.
- `just docker-build [tag]` — builds the container image via `.container/Dockerfile`, passing `GITHUB_TOKEN` as a
  build arg.

There is no "run a single test" shortcut in `justfile`; to scope a run, use
`mise exec --env it -- sbt "testOnly com.sneaksanddata.arcane.sql_server_change_tracking.tests.integration.StreamRunner"`
(tests are ZIO-test/ScalaTest specs under `src/test/scala/tests/integration`, and effectively all are integration
tests requiring the Docker Compose stack — there is no separate fast unit-test suite).

## Test suite notes

- `Test / parallelExecution := false` in `build.sbt` — specs run sequentially by design; don't try to parallelize.
- Integration tests assume live services at fixed localhost ports from `docker-compose.yaml`: MSSQL `1433`,
  Lakekeeper (Iceberg REST catalog) `20001`, Trino `8080`, Postgres `5432`. Credentials/URIs are hardcoded in
  `integration-tests.env` / `docker-compose.yaml` (e.g. MSSQL `sa`/`tMIxN11yGZgMC`).
- `integration-tests.sql` bootstraps the MSSQL schema/sample data and is run by the `setup_mssql` compose service.
- CI (`.github/workflows/build.yaml`) runs `docker compose up`, sources `integration-tests.env` into `$GITHUB_ENV`,
  then `sbt clean coverage test coverageReport`, then `sbt scalafmtCheckAll` — reproduce this order locally when
  validating changes (tests first, formatting check after).

## Formatting

- `.scalafmt.conf`: scalafmt `3.9.6`, `scala3` dialect, `maxColumn = 120`. Run `just check` before pushing.

## Release / CI flow (informational, don't invoke unless asked)

- Container images, Helm charts, and releases are all manual `workflow_dispatch` GitHub Actions
  (`build-container-image.yaml`, `build-graalvm-container-image.yaml`, `release.yaml`, `publish-ecr-public*.yaml`) —
  nothing auto-publishes on push to `main` except the test/coverage workflow.
- `release.yaml` only runs from `main` and uses semver bump inputs `major_v: 2`, `minor_v: 4` — keep these in sync
  with the actual next version if asked to cut a release.

## Helm / Kind

- `./.helm` is the chart for deploying this stream's CRDs/StreamClass on top of the separately-installed
  `arcane-crd` and `arcane-operator` charts. README has the full Kind walkthrough if you need to stand up a local
  cluster for manual verification.
