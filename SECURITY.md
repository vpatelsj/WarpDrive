# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability in WarpDrive, please report it
responsibly. **Do not open a public GitHub issue.**

Email: **security@warpdrive.dev** (placeholder — update with actual contact)

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We aim to acknowledge reports within 48 hours and provide a fix or
mitigation plan within 7 days for critical issues.

## Security Practices

### Secret Handling

- **No secrets in config files.** Auth credentials are resolved at runtime
  from environment variables (`${VAR}` expansion), managed identity (IMDS),
  or Azure Key Vault.
- **No secrets in logs.** The `log/slog` structured logger is configured to
  never emit credential material. Auth audit entries record provider name
  and success/failure but not token values.
- **Environment variable indirection.** For service principal auth, config
  specifies `client_id_env` and `client_secret_env` (the *names* of env
  vars), not the values themselves.

### Filesystem Permissions

- **`allow_other: false` (default).** Only the user running `warpdrive-mount`
  can access the FUSE filesystem. Enable `allow_other` only if non-root
  users (e.g., training jobs) need access, and ensure `/etc/fuse.conf`
  contains `user_allow_other`.
- **`CAP_SYS_ADMIN`** is the only Linux capability required (for FUSE
  mount). Drop all other capabilities in production. The Helm chart's
  DaemonSet is configured with `privileged: true` and `SYS_ADMIN` — review
  your security policy before deploying.
- **Mount propagation.** The Helm chart uses `Bidirectional` mount
  propagation so pods on the same node can see the mount. Understand the
  implications before enabling in multi-tenant clusters.

### Credential Rotation

- **Managed identity / OIDC federation** tokens are auto-refreshed before
  expiry. No manual rotation needed.
- **Key Vault secrets** are fetched on each credential resolution; rotate
  in Key Vault and WarpDrive picks up new values automatically.
- **Static credentials** (`auth.method: static`) should be rotated by
  updating the environment variables and restarting `warpdrive-mount`.

### Least Privilege

- Grant backends the minimum permissions needed:
  - Read-only access for training data buckets
  - Use scoped SAS tokens or IAM policies
- Avoid wildcard bucket policies
- Use separate service principals per backend when possible

### Network Security

- The metrics endpoint (default `:9090`) exposes operational data. Bind to
  `127.0.0.1:9090` or use network policies to restrict access in
  production.
- The control plane REST API (default `:8090`) should not be exposed
  externally. Use a service mesh or network policy.

## Dependencies

WarpDrive vendors all Go dependencies (`vendor/`). Review `go.sum` for the
full dependency tree. Key security-relevant dependencies:

- `github.com/Azure/azure-sdk-for-go` — Azure auth
- `github.com/aws/aws-sdk-go-v2` — AWS STS
- `github.com/golang-jwt/jwt/v5` — JWT validation
- `github.com/hanwen/go-fuse/v2` — FUSE kernel interface
- `github.com/rclone/rclone` — storage backend I/O

## Supported Versions

| Version | Supported |
|---------|-----------|
| main    | Yes       |

(No tagged releases yet.)
