# Security Policy

## Supported Versions

Security fixes are accepted for the current `main` branch. If release branches
are introduced, this file should be updated with explicit support windows.

## Reporting a Vulnerability

Please report suspected vulnerabilities through the repository security
advisory flow when available. If that is unavailable, contact the
maintainers through a non-public channel before posting details publicly.

Include:

- affected version or commit
- minimal reproduction steps
- relevant tool name
- stable error code or reason when available
- expected impact

Do not include real database credentials, raw production connection strings,
non-public hostnames, access tokens, or production query payloads.

## Scope

Security-sensitive areas include:

- SQL access-mode enforcement
- `admin_sql` exposure
- `execute_sql` compatibility exposure
- credential and DSN parsing
- TLS and `sslmode` handling
- metadata visibility policy
- telemetry redaction
- export artifact access
- external advisor command execution

## Public Issues

For public issues, use redacted placeholders and minimal reproductions. Keep the
technical signal, but remove identity, infrastructure, and credential details.
