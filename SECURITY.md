# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.15.x  | :white_check_mark: |
| < 0.15  | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to: [your-email@example.com]

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

Please include the following information:

- Type of issue (e.g. buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

## Security Features

### Build-time Security

- **Dependency Scanning**: Automated via Dependabot
- **SLSA Level 3 Provenance**: Build provenance for all releases
- **Signed Artifacts**: All releases signed with Sigstore/cosign
- **SBOM**: CycloneDX SBOM generated and signed for each release

### Runtime Security

- **Minimal Container Images**: Based on Debian bookworm-slim
- **Non-root Execution**: Container runs as non-root user
- **Dependency Management**: All dependencies pinned in Cargo.lock

### Supply Chain Security

- **OpenSSF Scorecard**: Automatically scanned weekly
- **Sigstore Transparency**: All signatures logged in public Rekor transparency log
- **GitHub OIDC**: Keyless signing with short-lived certificates

## Verification

### Verify Container Image Signature

```bash
cosign verify ghcr.io/jmacd/duckpond:latest \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com \
  --certificate-identity-regexp=^https://github.com/jmacd/duckpond/
```

### Verify .deb Package Signature

```bash
cosign verify-blob pond_0.15.0_amd64.deb \
  --bundle pond.deb.bundle \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com \
  --certificate-identity-regexp=^https://github.com/jmacd/duckpond/
```

### Verify SLSA Provenance

```bash
slsa-verifier verify-artifact pond \
  --provenance-path pond.intoto.jsonl \
  --source-uri github.com/jmacd/duckpond \
  --source-tag v0.15.0
```

### Verify SBOM

```bash
cosign verify-blob pond-sbom.json \
  --bundle pond-sbom.json.bundle \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com \
  --certificate-identity-regexp=^https://github.com/jmacd/duckpond/
```

## Security Practices

- All builds run in ephemeral GitHub-hosted runners
- No long-lived secrets or credentials
- Branch protection enabled on main branch
- Required status checks before merge
- Code review required for all changes
- Automated security scanning enabled

## Known Limitations

- No formal security audit has been conducted
- No bug bounty program
- Community-maintained project with volunteer contributors
