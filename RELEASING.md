# Releasing

`runnerq` follows [semver](https://semver.org) as described under
[Versioning policy](#versioning-policy), the same policy as the Go SDK
([runnerq-go's RELEASING.md](https://github.com/runnerq/runnerq-go/blob/main/RELEASING.md)).
Both SDKs release together under the same version number. Each release has
two kinds of notes:

- [`CHANGELOG.md`](CHANGELOG.md): the curated summary, written as PRs land,
  under `## Unreleased`.
- The GitHub release: that changelog section followed by the list of merged
  PRs GitHub generates from their labels
  ([`.github/release.yml`](.github/release.yml)).

The package is published to npm from CI. Tagging is the only manual step.

## While developing

- A PR that changes behaviour, the public API, the schema or the supported
  Node or Postgres versions adds a line to `## Unreleased` in
  `CHANGELOG.md`. Breaking changes say what breaks and how to move.
- Label every PR before merging, so the generated notes put it in the right
  section:

  | Label                                      | Section          | Bump it implies               |
  | ------------------------------------------ | ---------------- | ----------------------------- |
  | `breaking`                                 | Breaking changes | minor before 1.0, major after |
  | `feature`                                  | Features         | minor                         |
  | `fix`                                      | Bug fixes        | patch                         |
  | `perf`                                     | Performance      | patch                         |
  | `security`                                 | Security         | patch                         |
  | `docs`                                     | Documentation    | none on its own               |
  | `chore`, `refactor`, `ci`, `build`, `deps` | Maintenance      | none on its own               |
  | `skip-release-notes`                       | left out         | none                          |

  Unlabelled PRs land under "Other changes".

## Cutting a release

1. Pick the version from the labels merged since the last tag and the
   policy below.
2. In a PR, set the version and close the changelog section:

   ```bash
   npm version X.Y.Z --no-git-tag-version   # package.json and package-lock.json
   ```

   and rename `## Unreleased` in `CHANGELOG.md` to `## vX.Y.Z` (start a new
   empty `## Unreleased` above it).

3. After it merges, tag `main`:

   ```bash
   git checkout main && git pull
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

The `Release` workflow ([`.github/workflows/release.yml`](.github/workflows/release.yml))
runs on the tag:

1. It fails unless the tag equals `v` + the `version` in `package.json` and
   `CHANGELOG.md` has a section for the tag, before anything is published.
2. It runs `npm ci`, the unit tests and `npm run pack:check` (the tarball
   holds every exported file and none of the repository's tooling).
3. It runs `npm publish --provenance --access public`. A version with a
   prerelease part (`0.7.0-rc.1`) goes to the `next` dist-tag, anything else
   to `latest`.
4. It creates the GitHub release: the tag's section from `CHANGELOG.md`
   followed by the generated PR list. A tag with a prerelease suffix is
   published as a pre-release.

Every PR runs the same pack check and an `npm publish --dry-run` (the
`package` job in [`ci.yml`](.github/workflows/ci.yml)).

## Versioning policy

- **v0.x (now).** Pre-1.0, the minor version is the breaking-change
  boundary: a release with any `breaking` PR bumps the minor (v0.6 → v0.7),
  and so does one with new features. A patch release (v0.6.0 → v0.6.1) only
  fixes bugs and never breaks the API or the schema. npm's default range
  (`^0.6.0`) already stays within the minor.
- **v1.0.0 and later.** Breaking changes need a new major. Features bump the
  minor, fixes the patch.

### What the policy covers

- The Postgres schema: from v0.6 on, schema changes are additive migrations
  (new tables, new nullable columns), so a new SDK can run beside the
  previous minor during a rolling deploy. A change that cannot be additive is
  a breaking change.
- Supported runtimes: Node.js 22, 24 and 26, and the Postgres versions CI
  tests. Raising the Node floor or dropping a Postgres major is a breaking
  change.
- The public API, below.

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

### What it does not cover

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
