# Releasing DuckPond

This document describes the release process for DuckPond and addresses software supply chain security considerations.

## Current Release Process

### Automated Builds

All builds are performed via GitHub Actions (see `.github/workflows/rust-ci.yml`):

1. **On Pull Requests**: Run tests, clippy, and format checks only
2. **On Main Branch**: Run tests + build and publish container image + create .deb package
3. **On Version Tags** (`v*`): Same as main, with version-tagged artifacts

### Artifacts Produced

For each successful build on main or version tags:

- **Container Image**: `ghcr.io/jmacd/duckpond:latest` (or version tag)
  - Built with Podman on GitHub's Ubuntu runners
  - Based on Debian Bookworm (glibc 2.36)
  - Includes pond binary at `/usr/bin/pond`
  
- **Debian Package**: `pond_<version>_amd64.deb`
  - Available as GitHub Actions artifact (90 day retention)
  - Dependencies: libssl3, ca-certificates
  - Installs to `/usr/bin/pond`

### Creating a Release

1. **Update version** in `Cargo.toml` workspace section:
   ```toml
   [workspace.package]
   version = "0.16.0"
   ```

2. **Commit and tag**:
   ```bash
   git commit -am "Release v0.16.0"
   git tag v0.16.0
   git push origin main --tags
   ```

3. **Wait for CI**: GitHub Actions will automatically build and publish

4. **Download artifacts**: Go to Actions tab → select the workflow run → download artifacts

5. **Create GitHub Release**:
   - Go to Releases → Draft a new release
   - Select the tag
   - Attach the .deb package (download from artifacts first)
   - Document changes

## Software Supply Chain Security

### Current Security Measures

✅ **Build Isolation**: All builds run in ephemeral GitHub-hosted runners  
✅ **Dependency Pinning**: `Cargo.lock` is committed to the repository  
✅ **Audit Trail**: All builds are associated with specific commits and workflow runs  
✅ **Container Registry**: Uses GitHub Container Registry with authentication  

### Security Gaps (TODO)

❌ **No artifact signing**: Binaries and packages are not cryptographically signed  
❌ **No SBOM generation**: No Software Bill of Materials  
❌ **No provenance attestation**: No SLSA provenance  
❌ **Limited reproducibility**: Builds may not be bit-for-bit reproducible  

## Recommendations for Production Use

### 1. Sign Releases with GPG

Install GPG key in GitHub Actions:

```yaml
- name: Import GPG key
  uses: crazy-max/ghaction-import-gpg@v6
  with:
    gpg_private_key: ${{ secrets.GPG_PRIVATE_KEY }}
    passphrase: ${{ secrets.GPG_PASSPHRASE }}

- name: Sign .deb package
  run: |
    dpkg-sig --sign builder artifacts/*.deb
```

### 2. Generate SBOM with cargo-sbom

Add to workflow:

```yaml
- name: Generate SBOM
  run: |
    cargo install cargo-sbom
    cargo sbom > artifacts/pond-sbom.json
```

### 3. Add SLSA Provenance

Use the SLSA GitHub Generator:

```yaml
- uses: slsa-framework/slsa-github-generator/.github/workflows/generator_generic_slsa3.yml@v1.9.0
  with:
    provenance-name: pond-provenance.intoto.jsonl
```

### 4. Sign Container Images with Cosign

```yaml
- name: Sign container image
  run: |
    cosign sign --key env://COSIGN_KEY ${{ steps.meta.outputs.image_id }}:${{ steps.meta.outputs.version }}
  env:
    COSIGN_KEY: ${{ secrets.COSIGN_PRIVATE_KEY }}
```

### 5. Enable Dependabot

Create `.github/dependabot.yml`:

```yaml
version: 2
updates:
  - package-ecosystem: "cargo"
    directory: "/"
    schedule:
      interval: "weekly"
```

## Verifying Releases (For Users)

### Current State (No Signing)

**Trust model**: Trust GitHub Actions + repository access control

Verification steps:
1. Verify the artifact came from the official repository
2. Check the commit hash matches the release tag
3. Review the workflow run logs for any anomalies

### Future State (With Signing)

Users will be able to verify signatures:

```bash
# Verify .deb signature
dpkg-sig --verify pond_0.16.0_amd64.deb

# Verify container image
cosign verify ghcr.io/jmacd/duckpond:v0.16.0

# Verify SLSA provenance
slsa-verifier verify-artifact pond \
  --provenance-path pond-provenance.intoto.jsonl \
  --source-uri github.com/jmacd/duckpond
```

## Deployment to Production

### Target Environment

- **Platform**: Debian 12 (Bookworm)
- **Architecture**: x86_64 (amd64)
- **Minimum RAM**: 1GB (monitor memory usage)
- **Dependencies**: libssl3, ca-certificates

### Installation via .deb Package

```bash
# Download from GitHub Release
wget https://github.com/jmacd/duckpond/releases/download/v0.16.0/pond_0.16.0_amd64.deb

# Install
sudo dpkg -i pond_0.16.0_amd64.deb
sudo apt-get install -f  # Install any missing dependencies

# Verify
pond --version

# Uninstall
sudo dpkg -r pond
```

### Running in Production

Create systemd service `/etc/systemd/system/pond.service`:

```ini
[Unit]
Description=DuckPond Service
After=network.target

[Service]
Type=simple
User=pond
Group=pond
ExecStart=/usr/bin/pond <your-arguments>
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo useradd -r -s /bin/false pond
sudo systemctl daemon-reload
sudo systemctl enable pond
sudo systemctl start pond
sudo journalctl -u pond -f
```

## Security Considerations

### Runtime Security

- Run as non-root user (demonstrated in systemd service above)
- Use minimal container images (Debian bookworm-slim)
- Keep dependencies up to date (use Dependabot)
- Monitor for CVEs in dependencies

### Access Control

- Container registry requires authentication for writes
- GitHub Actions uses OIDC tokens (no long-lived credentials)
- Release artifacts require repo admin access to create

### Audit Trail

All releases are traceable:
- Git commit hash
- GitHub Actions workflow run ID
- Build logs (retained for 90 days)
- Container image layers and manifest

## Future Improvements

1. **Reproducible builds**: Investigate cargo-careful or similar tools
2. **Multi-architecture builds**: Add ARM64 support
3. **Automated security scanning**: Integrate trivy or grype
4. **FIPS compliance**: If required for regulated environments
5. **Release automation**: Auto-create GitHub releases from tags

## Questions or Issues?

For release-related questions, open an issue in the repository with the `release` label.
