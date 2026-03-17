# Security and Credentials Management: Design Study

## Problem Statement

DuckPond currently treats the host filesystem as a trust boundary and
delegates S3/R2 authorization to cloud IAM. There is no unified
security model -- credentials live in plaintext YAML configs (expanded
at `mknod` time), the `replicate` command emits secrets in base64 on
stdout, debug logging can leak credentials, and there are no internal
concepts of identity, authorization, or access control.

The two speculative design docs (`design-attestation-and-publishing.md`
and `design-cross-pond-import.md`) introduce Ed25519 signing keys,
device certificates, registration authorities, and multi-pond
attestation chains. These designs are thorough but introduce PKI
concepts without a coherent model for *how credentials and keys are
managed day-to-day*.

The user's core question: "Is there a system or dependency I should
adopt rather than designing security myself?"

## Current Security Posture

### What exists

| Area | Mechanism | Strength |
|------|-----------|----------|
| Data integrity | BLAKE3 per-file + bao-tree outboard streaming verification | Strong |
| Transfer integrity | ChunkedReader with BLAKE3 verification | Strong |
| S3 auth | access_key/secret_key in RemoteConfig | Functional but insecure storage |
| HydroVu auth | OAuth2 client_credentials via env templating | Better (env vars) |
| TLS | rustls for all HTTP/S3 connections | Strong |
| URL sanitization | Rejects embedded credentials in provider URLs | Good |

### What's broken or missing

| Problem | Severity | Location |
|---------|----------|----------|
| `mknod` expands env templates then stores plaintext secrets in the pond | High | `cmd/src/commands/mknod.rs` |
| `replicate` prints full credentials in base64 to stdout | High | `remote/src/factory.rs:654` |
| `pond sync --config=<base64>` accepts credentials on CLI (visible in `ps`) | High | `cmd/src/commands/control.rs` |
| Debug logging prints actual S3 config values | Medium | `remote/src/s3_registration.rs` |
| No signing keys, no attestation, no identity | Foundational | (nothing exists) |
| No access control, users, roles, or permissions | Foundational | (nothing exists) |
| Remote config YAML example has plaintext key/secret in comments | Low | `remote-config-local-push.yaml` |

## Design Principle: Adopt, Don't Invent

The user is not a security expert. Neither is this a product with a
security team. The correct approach is:

1. **Adopt existing standards and protocols** (C2SP signed-note,
   Ed25519, tlog-tiles) -- the attestation doc already does this well
2. **Adopt existing implementations** (RustCrypto ecosystem, keyring
   crate, age encryption) -- don't write crypto code
3. **Layer security onto the existing trust model** rather than
   replacing it -- the host filesystem trust boundary is correct for
   a single-operator tool
4. **Make the simple thing secure by default** -- if the user follows
   the obvious path, credentials should not leak

## Five Security Domains

### Domain 1: Credential Storage (Immediate)

**Problem**: S3 keys, OAuth secrets, and API tokens are stored in
plaintext inside the pond (after mknod template expansion) and emitted
in base64 on stdout (replicate).

**Adopt**: Two-tier approach.

**Tier A -- Environment variables (already partially done)**:
- Factory configs should *never* store expanded secrets in the pond
- Instead, store the template form: `{{ env(name='R2_SECRET') }}`
- At runtime, expand templates just-in-time before passing to
  object_store / OAuth client
- This is the Git-compatible model: config files can be committed
  because they contain no secrets

**Tier B -- OS keyring (new)**:
- Add the `keyring` crate (Apache-2.0/MIT) for secure credential
  storage on macOS Keychain, Linux secret-service, Windows Credential
  Manager
- `pond config set-secret <name> <value>` stores in OS keyring
- Config files reference: `{{ secret(name='r2_key') }}`
- A new `secret()` template function resolves from keyring at runtime
- Replicate command emits a config *without* credentials, plus
  instructions to set them up

**Tier C -- Encrypted config files (optional)**:
- For headless/CI environments without a keyring, support
  age-encrypted config sections
- The `age` crate (Apache-2.0/MIT) provides X25519 encryption
- Config files can contain `ENC[age,...]` markers for encrypted values
- Decryption key provided via `POND_AGE_KEY` env var or
  `~/.config/pond/age-key.txt`
- This is the SOPS model but simpler -- we only need value-level
  encryption, not full SOPS integration

**What NOT to build**:
- A secrets vault or server (too heavy)
- Full SOPS integration (external tool dependency)
- HashiCorp Vault integration (enterprise scope)

### Domain 2: Signing Keys and Attestation (Near-term)

**Problem**: The attestation design doc describes Ed25519 signing for
tlog checkpoints, manifests, and blog posts. No implementation exists.

**Adopt**: The attestation doc's design is sound. The key decisions
are already made:

- **Ed25519** via `ed25519-dalek` (BSD-3) for software keys
- **C2SP signed-note** format via `signed_note` crate (Apache-2.0)
  from Cloudflare Azul
- **`signature` trait** (Apache-2.0/MIT) as the abstraction boundary
- **`secrecy` crate** (Apache-2.0/MIT) for zeroizing key material
  in memory

**Key storage tiers** (from attestation doc -- correct as-is):
- Tier 1: Encrypted file on disk + keyring
- Tier 2: Cloud KMS (AWS KMS, GCP Cloud HSM)
- Tier 3: Hardware HSM via `cryptoki` (PKCS#11)

**What changes from the attestation doc**: The signing key should be
stored via the same credential infrastructure (Domain 1). The private
key file goes in `~/.config/pond/signing-key` (encrypted with age),
not as a raw file. The `POND_SIGNING_KEY` env var path remains for CI.

### Domain 3: Device Identity and PKI (Future)

**Problem**: The attestation doc describes device certificates for
regulated water systems -- devices sign data with TPM-backed keys,
authorities issue certificates.

**Adopt**: The attestation doc proposes C2SP signed-note format for
device certificates (not X.509). This is a reasonable choice for a
self-contained ecosystem. The key insight: **this is NOT traditional
PKI**. It's a purpose-built registration system using signed notes.

**What to adopt**:
- `tss-esapi` (Apache-2.0) for TPM 2.0 on production devices
- `ed25519-dalek` for software-only development devices
- The `signature` trait boundary already handles this

**What NOT to build now**:
- X.509 certificate infrastructure (defer until a regulator requires it)
- OCSP/CRL revocation checking (use signed-note revocation lists)
- A certificate authority server (the authority is just another pond)

**Key question for the user**: The attestation doc's approach of
"DuckPond is the tool for both the operator AND the regulator" is
elegant but ambitious. The MVP is: operator signs data with Ed25519,
publishes verifier key. Device attestation and authority infrastructure
can wait.

### Domain 4: Access Control and Authorization (Deferred)

**Problem**: DuckPond has no concept of users, roles, or permissions.
The host filesystem is the access control boundary.

**Analysis**: This is the right model for now. DuckPond is a
single-operator tool that runs on one machine. Adding multi-user
access control would require:
- User identity (who is making this request?)
- Permission model (who can read/write what paths?)
- Enforcement (where in the stack do we check?)

These are server-side concepts. DuckPond is a CLI tool, not a server.
The correct access control for a CLI tool is the OS:
- File permissions on `~/.config/pond/` protect signing keys
- File permissions on the pond directory protect data
- S3 IAM policies protect remote storage
- SSH/sudo controls who can run `pond` commands

**What to build (minimal)**:
- Audit trail in the control table: record which OS user ran each
  transaction (already partially done via `birth_username`)
- `pond config` should display the effective permissions model
  ("this pond is owned by user X, signing key is in keyring")

**What NOT to build**:
- Internal user/role/permission system
- Authentication server or API tokens
- Multi-tenant access control

If DuckPond ever becomes a networked service (e.g., the authority
registry), access control will matter. But that is a different product.

### Domain 5: Tamper Evidence and Auditability (Medium-term)

**Problem**: The Delta Lake log is unsigned. Someone with filesystem
or S3 access can tamper with data.

**Adopt**: The attestation doc's tlog-tiles design addresses this
comprehensively. The key pieces:

- Per-partition transparent logs (SHA-256 merkle trees)
- Signed checkpoints (C2SP tlog-checkpoint)
- Delta commit metadata with tlog checkpoints
- Parquet file checksums in Delta add actions

**What to prioritize**:
1. `pond attest` + `pond verify` (core loop)
2. Signed checkpoints per partition
3. Publish tiles to static site (sitegen integration)
4. Delta Lake commit-level hashing (closes the storage gap)

## Recommended Dependency Stack

### For Credential Management (Domain 1)
| Crate | License | Purpose |
|-------|---------|---------|
| `keyring` | Apache-2.0/MIT | OS keyring (macOS Keychain, Linux, Windows) |
| `age` | Apache-2.0/MIT | File encryption (for headless/CI key storage) |
| `secrecy` | Apache-2.0/MIT | Zeroize secrets in memory |
| `zeroize` | Apache-2.0/MIT | Zero memory on drop (transitive via secrecy) |

### For Signing and Attestation (Domain 2)
| Crate | License | Purpose |
|-------|---------|---------|
| `signature` | Apache-2.0/MIT | Generic Signer/Verifier traits |
| `ed25519` | Apache-2.0/MIT | Ed25519 trait definitions |
| `ed25519-dalek` | BSD-3 | Software Ed25519 implementation |
| `signed_note` | Apache-2.0 | C2SP signed-note format (Cloudflare Azul) |
| `tlog_tiles` | Apache-2.0 | Tlog tree + checkpoints (Cloudflare Azul) |

### For Device Attestation (Domain 3, future)
| Crate | License | Purpose |
|-------|---------|---------|
| `tss-esapi` | Apache-2.0 | TPM 2.0 bindings (when needed) |
| `cryptoki` | Apache-2.0/MIT | PKCS#11 HSM binding (when needed) |

## Phased Approach

### Phase 0: Fix Credential Leaks (immediate, prerequisite)
- Stop storing expanded secrets in the pond at `mknod` time
- Store the template form; expand at runtime
- Remove credential output from `replicate` -- emit config reference
  + setup instructions instead
- Redact all credential values in debug logging
- Add `secrecy::SecretString` for credential fields in RemoteConfig

### Phase 1: Credential Infrastructure
- Add `keyring` crate, implement `pond config set-secret` / `get-secret`
- Add `secret()` template function for config files
- Add `age` encryption for headless environments
- Update docs and examples to use env/keyring patterns

### Phase 2: Signing Key Management
- Add Ed25519 key generation (`pond config generate-signing-key`)
- Store private key in keyring or age-encrypted file
- Store public verifier key in pond config
- Implement `dyn Signer<ed25519::Signature>` trait boundary

### Phase 3: Attestation (from attestation doc)
- `pond attest` -- build tlog tiles, sign checkpoints
- `pond verify` -- verify against signed checkpoints
- Sitegen integration for publishing tiles

### Phase 4+: Device attestation, authority infrastructure, etc.
(As described in the attestation doc, when needed)

## Key Architectural Decision

**The security model is layered, not monolithic:**

```
+-------------------------------------------+
| Layer 4: Regulatory PKI (Domain 3)        |
|   Device certs, authority registration    |
|   -- DEFERRED until a regulator asks --   |
+-------------------------------------------+
| Layer 3: Data Attestation (Domain 5)      |
|   tlog-tiles, signed checkpoints,         |
|   tamper-evident logs, Delta integration  |
|   -- Phase 2-3 --                         |
+-------------------------------------------+
| Layer 2: Identity (Domain 2)              |
|   Ed25519 signing keys, C2SP signed-note  |
|   dyn Signer trait, key generation        |
|   -- Phase 2 --                           |
+-------------------------------------------+
| Layer 1: Credentials (Domain 1)           |
|   OS keyring, env vars, age encryption,   |
|   runtime template expansion, no leaks    |
|   -- Phase 0-1 (DO THIS FIRST) --        |
+-------------------------------------------+
| Layer 0: OS + Transport (existing)        |
|   File permissions, rustls TLS, S3 IAM    |
|   -- Already sound --                     |
+-------------------------------------------+
```

Each layer builds on the one below. You can't sign attestations
(Layer 3) without a signing key (Layer 2). You can't manage a signing
key without credential infrastructure (Layer 1). And Layer 1 depends
on the OS and transport security (Layer 0) that already exists.

## Open Questions for the User

1. **Headless environments**: The keyring crate requires a desktop
   session on Linux (DBus + secret-service). For BeaglePlay and CI,
   should we default to age-encrypted files, or env vars only?

2. **Replicate command redesign**: Should `replicate` emit a config
   file (with env var placeholders) that the user fills in? Or should
   it emit a `pond config set-secret` command sequence?

3. **Scope of Phase 0**: Should we fix credential leaks now (as a
   code change), or treat this entire document as a design study
   to be implemented later?

4. **Access control appetite**: Are you satisfied with "the OS is
   the access control layer" for the foreseeable future, or do you
   foresee multi-user pond access?
