#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
umask 077
mkdir -p .local
if [[ ! -f .local/demo.crt || ! -f .local/demo.key ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
    -config tls.conf -keyout .local/demo.key -out .local/demo.crt
fi
python3 - <<'PY'
import base64
import json
import os
from pathlib import Path
import secrets

env_path = Path(".local/demo.env")
if not env_path.exists():
    values = {
        name: secrets.token_hex(32)
        for name in ("PORTAL_CLIENT_SECRET", "APP_CLIENT_SECRET", "WORKER_CLIENT_SECRET", "KEYCLOAK_ADMIN_PASSWORD")
    }
    for name in ("OIDC_COOKIE_HASH_KEY", "OIDC_COOKIE_BLOCK_KEY"):
        values[name] = base64.b64encode(os.urandom(32)).decode()
    env_path.write_text("".join(f"{name}={value}\n" for name, value in values.items()))
else:
    values = dict(line.split("=", 1) for line in env_path.read_text().splitlines() if line)

template = Path("realm.template.json").read_text()
for name in ("PORTAL_CLIENT_SECRET", "APP_CLIENT_SECRET", "WORKER_CLIENT_SECRET"):
    template = template.replace("__" + name + "__", values[name])
realm = Path(".local/realm.json")
realm.write_text(json.dumps(json.loads(template), indent=2))
# Keycloak runs as a non-root container user and must read its import.
realm.chmod(0o644)
Path(".local/demo.crt").chmod(0o644)
print("Local TLS material and demo credentials are ready. No OS trust store was changed.")
print("Run: docker compose --env-file examples/oidc-demo/.local/demo.env -f examples/oidc-demo/compose.yaml up --build --wait -d")
PY
