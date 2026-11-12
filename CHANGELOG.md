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

### Breaking

- Storage that implements `ExecutorObserver` is no longer attached on its
  own; pass it to `worker.observe` like any other observer.
  `isExecutorObserver` is removed.
- The schema follows `runnerq-spec`'s neutral schema: `runnerq_worker_pools`
  is dropped.
