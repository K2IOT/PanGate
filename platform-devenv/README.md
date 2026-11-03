# platform-devenv

Local and dev environments for PanGate (spec §2.4, §9.1).

| Environment | What runs where | Commands |
|---|---|---|
| **local** | Gateway in your IDE (profile `local`); infra (Kafka, Redis — from M2) on `localhost` | `make bootstrap`, then run `GatewayApplication` with profile `local` (or `make run-local`) |
| **dev** | Everything in Docker | `make bootstrap images up smoke`, `make down` |

Dev secrets (TLS CA and cert, ES256 signing key, ticket keys) are generated on first start into
`~/.pangate-dev` (local) or the `dev-secrets` volume (dev). Trust `~/.pangate-dev/ca.crt` to connect from a
browser, or use `--ca`/`--insecure` with the devtools client.

```bash
TOKEN=$(java -jar gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar token \
  --key ~/.pangate-dev/es256.pem --tenant t1 --principal t1:me --device d1)
java -jar gateway-devtools/target/gateway-devtools-0.1.0-SNAPSHOT-exec.jar client \
  --url wss://localhost:8443/v1/ws --ca ~/.pangate-dev/ca.crt --token "$TOKEN" --device d1 --once
```

Checks: `scripts/local-start-check.sh <gateway-jar> <devtools-exec-jar>` and
`scripts/idle-memory.sh <gateway-jar> <devtools-exec-jar> [N]`.
