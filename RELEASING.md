# Releasing

`runnerq` is published to npm from CI. Pushing a `v*` tag runs
[`.github/workflows/release.yml`](.github/workflows/release.yml), which:

1. Fails unless the tag equals `v` + the `version` in `package.json`.
2. Runs `npm ci`, the unit tests and `npm run pack:check` (the tarball holds
   every exported file and none of the repository's tooling).
3. Runs `npm publish --provenance --access public`. A version with a
   prerelease part (`0.7.0-rc.1`) goes to the `next` dist-tag, anything else
   to `latest`.

Every PR runs the same pack check and an `npm publish --dry-run` (the
`package` job in [`ci.yml`](.github/workflows/ci.yml)).

## Cutting a release

```bash
git checkout main && git pull
npm version 0.6.0 --no-git-tag-version   # updates package.json and package-lock.json
git commit -am "Release v0.6.0"           # merge this through a PR
git tag -a v0.6.0 -m "v0.6.0"
git push origin v0.6.0
```

Tag the commit on `main` that carries the version bump.

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
