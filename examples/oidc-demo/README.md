# Production-style identity experience (local only)

This isolated Compose project demonstrates real Keycloak OIDC browser sign-in,
HTTPS, scoped JWT access tokens, and separate application/worker service identities.
It is not a hardened production deployment. The development-token demo on ports
8080/8090 is separate; this stack uses port 8443 and its own database/broker volumes.

## Start

Requirements: Docker Compose, OpenSSL, Python 3; Node and Playwright are needed only
for automated verification. From the repository root:

```sh
bash examples/oidc-demo/setup.sh
docker compose --env-file examples/oidc-demo/.local/demo.env \
  -f examples/oidc-demo/compose.yaml up --build --wait -d
```

Setup generates local client/admin secrets, independent cookie keys, and a
30-day self-signed certificate. Generated files are excluded from Git and Docker
build contexts. Re-running setup preserves credentials. Keycloak imports the realm
only on first creation; changing the template does not update an existing realm.
Keep the generated credentials and persistent Keycloak volume together.

| Entry point | URL |
| --- | --- |
| Zephyr platform | https://zephyr.localhost:8443 |
| Consumer application | https://checkout.localhost:8443 |
| Identity provider | https://identity.localhost:8443 |

`*.localhost` names resolve to loopback in Chrome/Chromium. Docker aliases resolve
the same names to the proxy. If your resolver does not support them, configure
those three names to resolve to `127.0.0.1` on your machine. Do not change issuer
names independently for browser and containers: token issuer validation requires
the same public issuer.

### Trust the local certificate

Setup **does not modify your OS trust store**. Until you trust the generated
certificate, your browser will show a certificate warning. For this demo only,
import `.local/demo.crt` into your user certificate trust store. On macOS, open it
in Keychain Access, add it to the login keychain, open its Trust section, and trust
it for SSL. Follow your organization's local-development certificate policy.
Remove that trust when finished. Never trust a certificate from an unknown source.

Containers trust the exact demo certificate using `SSL_CERT_FILE`; certificate
verification is not disabled. The API verification script also validates this
certificate. Its browser automation explicitly accepts the self-signed certificate
in an isolated test context; this does not configure your normal browser.

Production requires a trusted certificate and managed TLS/secret rotation.
Do not deploy the self-signed demo key or realm credentials publicly.

## Experience as a platform operator

1. Open the platform URL. It redirects to Keycloak, not a token input.
2. Sign in as **operator**, password **demo-local-only**.
3. You can inspect workflows, start runs, and **Register workflow** from a source
   file or pasted DSL. Choose a new positive version and download generated
   contracts; there is no platform restart. The token field is hidden.
4. Open the consumer application, place an order with a unique ID, and inspect
   its run ID in the platform UI.
5. Open a private browser window and sign in as **viewer**, same demo password.
   Reads/downloads are allowed; start/register controls are hidden, and direct
   mutation requests return 403.

Both users are intentionally disposable local users. MFA is not configured.
The generated Keycloak admin password is in `.local/demo.env` under
`KEYCLOAK_ADMIN_PASSWORD`; the admin username is `demo-admin`. Do not use the
admin account as the application's identity.

## Machine identities

The checkout backend uses `checkout-app` with workflow read/start permissions.
Its worker uses `checkout-worker` with worker-execute permission only. No identity
in the demo gets `zephyr:admin`.

The sample uses the SDK's `NewClientCredentialsSource` to obtain/cache access
tokens and fetch replacements near expiry. This is the OAuth **client credentials**
grant, not a refresh-token grant. Portal sessions use the provider's refresh-token
grant, automatically renewing access tokens near expiry.
Idle worker polls have a 25-second deadline, shorter than the proxy timeout, and
repeat normally without treating an idle queue as a dependency error.

Keycloak's mapper emits permissions as top-level `roles`, which Zephyr recognizes,
and sets audience `zephyr-api`. Requested scopes do not grant roles a user lacks.
Browser sign-in uses authorization code + PKCE with nonce/state checks.
Session cookies are Secure, HttpOnly, SameSite=Lax opaque random IDs. Access and
refresh tokens are signed/encrypted in shared PostgreSQL storage, never exposed
to browser JavaScript. PostgreSQL row locks serialize refresh across replicas.
Keycloak refresh-token rotation is enabled with zero reuse. Portal sessions have
an absolute eight-hour maximum; provider expiry/revocation may end them earlier.
Without a refresh token, the session lasts only until access-token expiry.
Temporary provider/storage failures return 503 without destroying the session;
invalid refresh grants require signing in again.

Apply the updated migration job before updating an existing demo:

```sh
docker compose --env-file examples/oidc-demo/.local/demo.env \
  -f examples/oidc-demo/compose.yaml build server migrate checkout-app
docker compose --env-file examples/oidc-demo/.local/demo.env \
  -f examples/oidc-demo/compose.yaml run --rm migrate
docker compose --env-file examples/oidc-demo/.local/demo.env \
  -f examples/oidc-demo/compose.yaml up --wait -d --no-deps --force-recreate server checkout-app
```

Existing Keycloak realms are not re-imported: add the realm role
`zephyr:workflow:register` and assign it to operator; enable Revoke Refresh Token
with Max Reuse 0 in realm token settings. Newly created realms include these.

RabbitMQ credentials remain independent of these API identities.

## Reproduce verification

```sh
npm ci --prefix test/browser
npm exec --prefix test/browser -- playwright install chromium
node examples/oidc-demo/verify.mjs
node examples/oidc-demo/verify-cli.mjs
```

Each test takes roughly two minutes. The portal test validates:

- Real OIDC login and serving authenticated HTML/JS/CSS.
- Secure session cookie and hidden development token field.
- Permission-aware viewer UI and 403 on both start and registration.
- Live registration, immutable version conflicts, version-pinned contract ZIPs,
  and execution by the consumer's existing worker.
- Separate service JWT roles, unauthorized requests (401), and scope denials.
- Public proxy blocking `/metrics`.
- Full consumer workflow before and after token expiry.
- Portal renewal after 60-second access-token expiry; stale standalone API JWT (401).
- Persistence of session and runtime registration across a server restart.
- Local session revocation and remaining provider SSO behavior.

Results are saved to `.local/verification-report.json`, without access tokens or
client secrets. Run this before calling the setup successful; a healthy container
alone does not establish successful identity integration.

The CLI test validates all seven consumer workflow commands, automatic
browser-approved device sign-in, private session storage, concurrent-process
refresh rotation, provider token revocation, and viewer authorization. Its
secret-free report is `.local/cli-verification-report.json`. New realms include
the public `zephyr-cli` device-flow client; existing realms need that client added
as described in the [CLI guide](../../docs/CLI.md).
The platform advertises issuer/client settings at public `/auth/config`, so CLI
sign-in needs only `--endpoint` on trusted HTTPS. This self-signed local demo also
requires either OS certificate trust or an explicit `--ca-file`; issuer is
discovered automatically.

## Observed limitations and gaps

| Area | Current behavior / remaining work |
| --- | --- |
| Portal refresh | Implemented, including rotated refresh tokens. Absolute session/provider lifetimes still require re-login; production key rotation and provider outage operations need planning. |
| Logout | Zephyr deletes its server-side session and clears its cookie, not the provider session. Signing in again can immediately succeed without credentials. Provider logout is not integrated. |
| Viewer UX | Mutation controls follow verified permissions; the backend remains authoritative. Role changes take effect when a newly issued access token reflects them. |
| Authorization isolation | Scopes govern API action categories, not tenant/workspace boundaries or individual workflow ownership. |
| Registration | Live immutable registration and contract downloads are implemented. Workers and service credentials are still deployed/provisioned separately; a Helm chart is not available. |
| Keycloak operations | Demo uses `start-dev` and embedded storage. Production needs an operationally managed provider, HA/backups, MFA/policies, and credential rotation. |
| Dependency transport | HTTPS covers browser/API/provider traffic. Internal PostgreSQL and RabbitMQ still use plaintext local-network connections and local sample credentials. |
| Consumer security | The sample consumer UI/API has no user authentication and is bound behind a loopback-only proxy. It is not a production customer application. |
| Secrets | Demo uses a local env file, not a production secret manager. Cookie/client key rotation needs an operational strategy. |
| Release readiness | This does not prove target-cluster capacity, PITR, monitoring, or the production release gates. |

A real-provider test also uncovered and fixed a portal routing bug: after login,
the OIDC handler delegated static pages to the gateway instead of embedded portal
assets. A regression test now verifies authenticated HTML, JavaScript, and CSS.

## Stop and inspect

```sh
docker compose --env-file examples/oidc-demo/.local/demo.env \
  -f examples/oidc-demo/compose.yaml logs -f server checkout-app

docker compose --env-file examples/oidc-demo/.local/demo.env \
  -f examples/oidc-demo/compose.yaml stop
```

Stop preserves all volumes and credentials. Do not remove the local credentials
while reusing the imported Keycloak realm: the client secrets would no longer
match. Certificates expire after 30 days; renew and update trust deliberately.
