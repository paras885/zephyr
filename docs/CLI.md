# Consumer CLI

Zephyr provides one consumer CLI, `zephyr`, for authoring, validation, artifact
generation, publication, invocation, and inspection. It replaces `zephyr-gen`.

Build/install from the repository:

```sh
go install ./cmd/zephyr
# Or: go run ./cmd/zephyr --help
```

The installed executable is in Go's bin directory (`go env GOPATH` plus `/bin`
unless `GOBIN` is set). Put that directory on your PATH.

## Workflow commands

Run these in your application's directory:

```sh
# Creates workflow.zephyr in the current directory (a worker-free Hello workflow).
zephyr workflow generate
zephyr workflow validate

# Writes contracts and .env.example in the current directory.
zephyr workflow artifacts --version 1

# Publishes source/version and writes the returned artifacts in the current directory.
zephyr workflow publish --version 1 --force

# Latest registered version; explicit --version pins a version.
zephyr workflow start Hello --input '{"value":"hello"}' --idempotency-key hello-1
zephyr workflow status workflow-123
zephyr workflow runs Hello --limit 10 --offset 0 --status COMPLETED
```

Use `--file path/to/definition.zephyr` for generate/validate/artifacts/publish,
and `--output ./generated` for artifacts/publish. Start accepts `--input @input.json`.
Flags can precede or follow identifiers. `runs` returns a JSON page with `items`,
`total`, `limit`, and `offset`; limit is 1-100, default 25. Page with `--offset`
for more results. `status` includes context, result, tasks, events, and failures.
Online results and validation results go to stdout as JSON; login instructions
and errors go to stderr. Failed commands exit nonzero. Ctrl-C cancels sign-in.

Existing files are never overwritten without `--force`; symlinks and directories
cannot be overwritten as artifacts. Publishing preflights local outputs before
registration. If disk writes fail after the server has committed registration,
the CLI explicitly reports partial success; rerun the identical source/version
with `--force`. Each file is saved by atomic replacement, but the artifact set
and server registration are not a distributed transaction.

Publishing is immutable by name/version: identical replay is idempotent, changed
content returns 409. Use a new positive version for changes. It produces Go client,
models and worker interfaces; Python models/interfaces; TypeScript models; and a
non-secret `.env.example`. Publish fills the endpoint in that template but never
writes tokens into application artifacts. Artifact generation alone is offline.
Generated clients pin the chosen version. Publishing does not deploy workers.

## Production sign-in

Configure a **separate public OIDC client**, default ID `zephyr-cli`, with OAuth
Device Authorization Grant enabled. Do not reuse the confidential portal client,
embed a client secret, or enable password grants. The provider must advertise
`device_authorization_endpoint` and `token_endpoint` in discovery, issue OIDC ID
tokens and expiring access tokens, and include the platform API audience and
permissions in access tokens. Zephyr needs:

- `zephyr:workflow:read` for status/runs.
- `zephyr:workflow:start` for start.
- `zephyr:workflow:register` for publish.

These are permissions assigned by the provider, not grants created by asking for
scopes. The CLI requests `openid profile email`; providers may need corresponding
client scopes/mappers. Enable refresh-token issuance and rotation on the provider.

```sh
zephyr auth login --endpoint https://zephyr.example
zephyr auth status
zephyr auth logout
```

The platform serves public `GET /auth/config` metadata containing `issuer`,
`client_id`, and the device grant type (no secrets). The CLI discovers these over
verified HTTPS; the consumer does not need to know the provider's issuer or client
ID. Platform operators set `OIDC_ISSUER_URL` and optionally `OIDC_CLI_CLIENT_ID`
(default `zephyr-cli`). Provision that public client at the provider separately.
For older platforms without discovery, `--issuer` and `--client-id` remain
explicit overrides. Failed or invalid discovery is reported, not silently
replaced with guessed provider settings.

Explicit login always starts a fresh sign-in, allowing recovery from a rejected
access token or switching users (log out first to revoke the old CLI grant).
Login displays a verification URL/code and opens a browser. Authenticate with your
provider (including MFA, if configured) and approve the CLI. On a headless machine,
use `--no-browser` and open the displayed URL on another device. The CLI verifies
the signed ID token for its issuer/client audience before saving the session.
Online commands start this flow automatically when a session is absent or
expired, provided the endpoint supports auth discovery (or issuer was explicitly
configured). They reuse/renew sessions
without a browser until the provider revokes/expires them or the eight-hour
absolute CLI session limit is reached. A revoked refresh grant starts sign-in
again; a temporary renewal failure retains the session and returns an error.
An API 401 (including invalid audience) directs you to login/check configuration;
the CLI does not silently retry mutation requests.

Session settings default to the OS user-config directory under `zephyr/session.json`.
Override with `ZEPHYR_CONFIG_DIR` (a private directory). The directory must be
0700 and the session file 0600. **Tokens are stored in that private local file,
not encrypted by an OS keychain.** Use a secured workstation/encrypted disk; do
not share, commit, or include it in artifacts/backups exposed to other users.
Windows users must additionally restrict the directory's ACL to their account.
One profile is supported currently; signing in to a different endpoint replaces
it, and tokens are never reused across different endpoint/issuer/client settings.

Atomic file replacement and an exclusive directory lock serialize refresh across
CLI processes, preventing reuse of rotated tokens. After a killed/crashed process,
confirm no CLI is running before removing the specifically reported
`session.json.lock` directory. Do not remove locks while another process signs in.
Locks/sign-in have a five-minute timeout.

Logout revokes the refresh token (or access token if no refresh token) when the
provider advertises revocation, then deletes local credentials. A provider error
retains the session so revocation can be retried; an unsupported revocation
endpoint produces an explicit local-only warning. Logout does not end browser SSO.
`auth status` reports local metadata, not a provider revocation check, and never
prints tokens.

Settings: `ZEPHYR_ENDPOINT`, `ZEPHYR_OIDC_ISSUER`, `ZEPHYR_OIDC_CLIENT_ID`,
`ZEPHYR_CA_FILE`, or equivalent flags. Saved endpoint/issuer/client/CA settings
are reused. HTTPS is required except loopback HTTP for development. Additional CA
files extend system trust; there is no insecure TLS flag. A production platform
and identity provider with publicly trusted certificates require **no CA flag**.
For private CAs, install the CA in the OS trust store according to your
organization's policy or explicitly provide `--ca-file`. Discovery never
downloads/installs certificate trust. Switching platform endpoints discards the
previous issuer/client/CA settings and tokens unless explicitly overridden.

For CI/service automation, explicitly supply `ZEPHYR_TOKEN` with a scoped access
token from your service identity. This overrides interactive sessions for workflow
commands; the CLI does not refresh externally supplied tokens. In the explicitly
enabled development platform it can hold the development token instead.

## Local production-style demo

New realms in [the OIDC demo](../examples/oidc-demo/README.md) include the public
`zephyr-cli` device-flow client. For an existing realm, create that client in
Keycloak, enable OAuth 2.0 Device Authorization Grant, and assign the default
`profile`, `email`, and `zephyr-claims` scopes (full scope allowed). Operator and
viewer retain their existing action permissions.

```sh
zephyr auth login \
  --endpoint https://zephyr.localhost:8443 \
  --ca-file /absolute/path/to/examples/oidc-demo/.local/demo.crt
```

Authenticate as `operator` / `demo-local-only`. The saved CA setting is used by
subsequent commands. Authenticate as `viewer` to verify read-only behavior.
If the demo certificate is already trusted by your OS, omit `--ca-file` too.
The demo's tokens last 60 seconds, so waiting 70 seconds exercises CLI refresh.
