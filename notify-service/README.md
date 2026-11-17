# notify-service

Push tokens per device and notifications to APNs / FCM for every tenant (spec
`docs/superpowers/specs/2026-11-17-offline-notifications-design.md`, ADR 0040). It knows nothing about chat: a tenant
backend produces `NotifyRequest` (contracts `notify.proto`) on `c1.notify.requests`, and notify-service sends it to
every registered device of the recipient.

## Run

`make images up` starts it with the devenv (`notify`, public API on host port 8094, actuator/ready on 8095), sending to
the `fake-apns` (9550) and `fake-fcm` (9551) stand-ins of gateway-devtools; throwaway keys are generated at start
(`notify.dev-credentials`). `make smoke` runs the `CHAT notify` step through it.

## API

`Authorization: Bearer <connect token>`; tenant, principal and device come from the token.

| Endpoint | |
|---|---|
| `PUT /v1/devices/self/push` | `{platform: apns\\|fcm, token, environment?: production\\|sandbox}` → `204`; replaces the device's token and removes the same token from any other device. APNs tokens are 64–200 hex characters, FCM ≤ 4 096. 10 per minute per device (`429`) |
| `DELETE /v1/devices/self/push` | `204` |

Errors are `application/problem+json` with `code` `invalid_request`, `invalid_token`, `rate_limited`, `unavailable`.

## Delivery

- Group `notify` on `c1.notify.requests`; a request is checked (`NotifyRules`), dropped when expired, deduplicated by
  `request_id` (Redis, 1 h; Redis down sends anyway), then sent to each token of the principal (or one device).
- APNs: HTTP/2, provider token ES256 (`.p8`, renewed every 50 min), `apns-topic` = bundle id, sandbox tokens to the
  sandbox host. FCM: HTTP v1 with an OAuth2 access token of the service account.
- Dead tokens (APNs 410 / `BadDeviceToken`, FCM `UNREGISTERED`) are removed if the device still holds them; 429 / 5xx
  / no answer are tried 3 times in all (1 s, 4 s or `Retry-After`), never past `expires_at_ms`; refused credentials
  are logged and counted.
- Group `notify-revocations` on `gw.control` removes the tokens of a revoked device or principal; a daily job removes
  tokens not registered again for 180 days.

## Configuration (`notify.*`)

`kafka.bootstrap`, `kafka.requests-topic` (`c1.notify.requests`), `kafka.control-topic` (`gw.control`), `redis.uri`,
`auth.issuer`, `auth.jwks-uri`, `max-in-flight` (1000), `attempts` (3), `token-idle-ttl` (180d),
`registrations-per-minute` (10), and per tenant:

```yaml
notify:
  tenants:
    chat:
      apns: { key-file: /secrets/apns.p8, key-id: ABC123, team-id: TEAM123, bundle-id: com.example.chat }
      fcm: { project-id: example-chat, service-account-file: /secrets/fcm.json }
```

## Metrics

`notify.requests{result}` (sent, no_token, expired, duplicate, invalid, no_config), `notify.sends{platform,result}`,
`notify.tokens.removed{reason}` (provider, revoked, replaced, idle), `notify.send.latency{platform}`,
`notify.provider.auth_failed{platform}`.
