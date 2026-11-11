# Releasing

`runnerq` follows [semver](https://semver.org), with the same policy as the
Go SDK ([runnerq-go's RELEASING.md](https://github.com/runnerq/runnerq-go/blob/main/RELEASING.md)).
Both SDKs release together under the same version number. Release notes are
drafted by [release-drafter](https://github.com/release-drafter/release-drafter)
from merged PR titles and labels, and the package is published to npm from
CI. Tagging is the only manual step.

## How notes get drafted

Every push to `main` runs
[`.github/workflows/release-drafter.yml`](.github/workflows/release-drafter.yml),
which:

1. Looks at every PR merged since the last tag.
2. Categorizes each by label (see
   [`.github/release-drafter.yml`](.github/release-drafter.yml)).
3. Resolves the next semver bump (`major` / `minor` / `patch`) from the
   highest-impact label across that batch.
4. Updates an unpublished draft release named after that version with the
   formatted notes.

The suggested version is a hint: the tag you push decides the version (see
the v0.x policy below, where a breaking change bumps the minor).

On PR events the same workflow runs an autolabeler that maps Conventional
Commit prefixes in the PR title to labels:

| PR title prefix                      | Auto-label | Bumps     |
| ------------------------------------ | ---------- | --------- |
| `feat:`                              | `feature`  | minor     |
| `fix:`                               | `fix`      | patch     |
| `perf:`                              | `perf`     | patch     |
| `docs:`                              | `docs`     | patch     |
| `chore:`                             | `chore`    | patch     |
| `refactor:`                          | `refactor` | patch     |
| `security:`                          | `security` | patch     |
| `build:`                             | `build`    | patch     |
| `ci:`                                | `ci`       | patch     |
| `deps:` / `chore(deps):`             | `deps`     | patch     |
| Anything ending `!:` (e.g. `feat!:`) | `breaking` | **major** |
| Body contains `BREAKING CHANGE`      | `breaking` | **major** |

A PR whose title has no prefix gets no label: set one by hand. The
autolabeler doesn't strip labels that are already there, so a manual label
also overrides it. `skip-release-notes` leaves a PR out of the notes.

[CHANGELOG.md](CHANGELOG.md) is the curated summary next to the drafted
notes. Add an entry under `## Unreleased` in any PR that changes behaviour
users can see, with the upgrade step if it breaks something.

## Cutting a release

Once `main` is in a state you want to release:

1. Open a PR that sets the version and closes the changelog section:

   ```bash
   npm version 0.6.0 --no-git-tag-version   # package.json and package-lock.json
   ```

   and rename `## Unreleased` in CHANGELOG.md to `## v0.6.0 (YYYY-MM-DD)`,
   with a fresh empty `## Unreleased` above it.

2. Merge it, then tag that commit on `main`:

   ```bash
   git checkout main && git pull
   git tag -a v0.6.0 -m "v0.6.0"
   git push origin v0.6.0
   ```

The `Release` workflow ([`.github/workflows/release.yml`](.github/workflows/release.yml))
fires on the tag push:

1. It fails unless the tag equals `v` + the `version` in `package.json`.
2. It runs `npm ci`, the unit tests and `npm run pack:check` (the tarball
   holds every exported file and none of the repository's tooling).
3. It runs `npm publish --provenance --access public`. A version with a
   prerelease part (`0.7.0-rc.1`) goes to the `next` dist-tag, anything else
   to `latest`.
4. It finalizes the draft release (matching the tag and name) and publishes
   it.

Every PR runs the same pack check and an `npm publish --dry-run` (the
`package` job in [`ci.yml`](.github/workflows/ci.yml)).

## Versioning policy

- **v0.x**: pre-1.0. The API may change between minors; a patch release
  never breaks. Breaking changes are listed under "Breaking" in the
  changelog with the upgrade step.
- **v1.0.0+**: stable public API. Breaking changes require a new major
  version.

The Postgres schema only grows by additive migrations (new tables and
nullable columns), so a new SDK version can run beside an old one on the same
database during a rolling deploy. Removing or changing a column is a breaking
change.

The "public API" surface is what the package's `exports` map exposes:

- `runnerq`: `activity`, `runner`, `RunnerQClient`, `ActivityHandle`,
  `ActivityContext`, `Worker`, the options, error classes and the types
  exported with them.
- `runnerq/storage`: the storage contract (interfaces and the supporting
  data types) a custom backend implements.
- `runnerq/postgres`: `PostgresStorage` and `PostgresConfig`.
- `runnerq/conductor`: `startAgent`, `Agent`, `AgentConfig` and the wire
  error types.

Files under `dist/` that the `exports` map doesn't name, `src/` internals and
the `examples/` directory carry no stability guarantee.

## What is _not_ covered by stability guarantees

- Database schema columns added defensively for future use, where a
  breaking change to the column would also bump the major.
- Operational defaults such as the shutdown grace period, lease and poll
  intervals. They may change.
- Exact wording of log and error messages. Error classes are stable;
  human-readable messages are not.

## Pre-release flow (optional)

To ship a `vX.Y.Z-rc.N` beside the stable line, set that version and push
the matching tag: the same workflow publishes it under the `next` dist-tag,
so `npm install runnerq` keeps getting the latest stable version and users
opt in with `npm install runnerq@next`. The GitHub release is marked as a
pre-release.

## One-time npm setup

The workflow authenticates with
[npm trusted publishing](https://docs.npmjs.com/trusted-publishers): npm
trusts an OIDC token that GitHub issues to this repository's `release.yml`,
so no long-lived npm token is stored in GitHub. Provenance is attached to
every version and shown on npmjs.com.

npm only lets you add a trusted publisher to a package that already exists,
so the first version is published with a token:

1. On npmjs.com, as an owner of the `runnerq` organization or account that
   will own the package, create a granular access token with read and write
   access to packages, valid for a few days.
2. Add it to this repository as the Actions secret `NPM_TOKEN`
   (Settings → Secrets and variables → Actions).
3. Push the first tag (`v0.6.0`). The workflow publishes with the token, still
   with provenance.
4. On npmjs.com, open `runnerq` → Settings → Trusted publishing, choose
   GitHub Actions and enter organization `runnerq`, repository `runnerq-ts`,
   workflow filename `release.yml` (leave the environment empty).
5. In the same settings page, set publishing access to require two-factor
   authentication and disallow tokens. Delete the `NPM_TOKEN` secret and
   revoke the token.

From then on a tag push is all a release needs. If npmjs.com lets you add a
trusted publisher before the package exists by the time you do this, skip the
token: do step 4, then push the tag.

## After publishing

1. Check the version page on npmjs.com shows the provenance badge, and that
   `npm install runnerq@X.Y.Z` works in an empty project.
2. Tag the Go SDK at the same version if it isn't already.
3. Announce the release; the GitHub release URL is
   `https://github.com/runnerq/runnerq-ts/releases/tag/vX.Y.Z`.
