# Security policy

## Reporting a vulnerability

Please don't open a public issue. Report it privately through GitHub:
[open a private vulnerability report](https://github.com/runnerq/runnerq-ts/security/advisories/new)
(the repository's Security tab → Report a vulnerability).

Include the affected version, what an attacker can do, and the steps or code
to reproduce it. We'll acknowledge the report, keep you posted while we work
on a fix, and agree a disclosure date with you. With your consent we credit
you in the advisory.

A vulnerability in the Go SDK goes to
[runnerq-go](https://github.com/runnerq/runnerq-go/security/advisories/new),
and one in the shared schema or protocol to
[runnerq-spec](https://github.com/runnerq/runnerq-spec/security/advisories/new).
If you aren't sure which, pick any of them.

## Supported versions

Until 1.0, fixes go into the latest minor release only. Upgrade to it to get
them.

| Version          | Supported |
| ---------------- | --------- |
| latest 0.x minor | yes       |
| older 0.x minors | no        |

## Scope

In scope: the `runnerq` package and everything under its `exports` (the
worker, client, Postgres storage and conductor agent), and the SQL it runs.

Out of scope: vulnerabilities in PostgreSQL or Node.js themselves, and
problems that need an attacker who can already write to the RunnerQ tables
or run code in the worker process.
