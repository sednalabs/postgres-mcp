# Optional CI Dependency Setup

Some repository configurations use companion workspaces or external probe
projects during CI. Public releases should document the expected interface
without embedding non-public repository names, release names, or credential
values.

## Recommended Pattern

1. Prefer published packages or public source dependencies for release CI.
2. If a companion workspace is required, pin it by commit or version.
3. Inject any required read-only credential through the CI secret store.
4. Never print credential values or clone URLs with embedded credentials.
5. Allow forked or secretless contexts to skip optional integration lanes while
   still running checks that do not require restricted access.

## Workflow Requirements

CI lanes that depend on external projects should document:

- dependency purpose
- expected checkout path
- pin or version
- whether the lane is required or advisory
- behavior when credentials are unavailable
- local fallback command when practical

## Public Release Posture

Before publishing, remove or neutralize:

- non-public repository names
- non-public release asset names
- hard-coded organization or user names
- credential variable names that reveal non-public infrastructure
- clone URLs with embedded credentials

Use placeholders in examples:

```bash
gh secret set <CI_READ_ONLY_CREDENTIAL> \
  --repo <owner>/<repo> \
  --app actions \
  --body "$CI_READ_ONLY_CREDENTIAL"
```

## Verification

1. Confirm required public checks run in a clean checkout.
2. Confirm optional credentialed lanes either run with configured credentials or
   skip clearly in secretless contexts.
3. Confirm workflow summaries do not reveal non-public dependency names or
   credential values.
