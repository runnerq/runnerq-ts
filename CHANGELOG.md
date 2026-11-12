# Changelog

All notable changes to `runnerq` are recorded here. The project follows
[semver](https://semver.org); see [RELEASING.md](RELEASING.md) for what that
means before 1.0. Each GitHub release is this file's section for the tag
followed by the PR list GitHub generates from labels.

## Unreleased

The first release on npm. It will be tagged v0.6.0, the same version as the
Go SDK's next release.

### Added

- `runnerq` is published to npm, with provenance, from CI on a `v*` tag.
  Install with `npm install runnerq` instead of the GitHub URL.
- A release process: this changelog, `RELEASING.md`, and GitHub releases
  made from the changelog plus label-generated notes, as in the Go SDK.
- Security checks in CI (`npm audit` on runtime dependencies, CodeQL),
  Dependabot, and `SECURITY.md` with private vulnerability reporting.
- An upgrade test in CI: the previous version creates the schema and parks
  work (a sleep, a signal wait, a parent waiting on a child, a delayed
  start); this version migrates the database and must finish it, running
  each remaining step once. CI also covers PostgreSQL 14 to 18.
- Observability: a `logger` option (structured JSON lines on stderr by
  default, the same messages and fields as the Go SDK; `silentLogger` turns
  it off), labelled metrics (`LabeledMetrics`) with gauges for queue depth,
  oldest pending age and activities in flight (`gaugeIntervalMs`, off by
  default), and a `Tracer` option on `Worker` and `RunnerQClient` that
  carries the W3C `traceparent` in activity metadata. OpenTelemetry is the
  `runnerq/otel` export, with `@opentelemetry/api` as an optional peer.

### Breaking

- Storage that implements `ExecutorObserver` is no longer attached on its
  own; pass it to `worker.observe` like any other observer.
  `isExecutorObserver` is removed.
- The schema follows `runnerq-spec`'s neutral schema: `runnerq_worker_pools`
  is dropped.
- Metric names are runnerq-spec's catalog names, not the worker's event
  names. Workers log to stderr by default.
