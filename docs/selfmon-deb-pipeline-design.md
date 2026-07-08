# Selfmon `.deb` Auto-Update Pipeline (Design)

Status: **Design only** -- no code or infrastructure has been changed yet.
Decision locked: publish the `pond` Debian package as a **ghcr OCI artifact**
pulled with `oras`, with **latest/prod promotion parity** matching the container
images.

This document describes how to automate updates for the natively-installed
`watershop-selfmon` pond so it stays current the same way the containerized
production ponds do.

## Sequencing and naming (relative to the watertown rename)

This pipeline is built **after** the `duckpond` -> `watertown` rename lands
(see `docs/watertown-migration-plan.md`, Part 4). The sequence is deliberate:
the rename is a mechanical, low-risk sweep, whereas this pipeline introduces
brand-new, unverified CI and deployment surface. Building the pipeline once, on
a stable `watertown` base, avoids authoring it under `duckpond` names and then
sweeping it through the rename.

Naming consequences (all post-rename `watertown` identifiers):

- The new deb artifact package is `ghcr.io/jmacd/watertown/pond-deb`. Its path
  follows the renamed image package `ghcr.io/jmacd/watertown/watertown`.
- The Debian package name becomes `watertown` and vendor assets install to
  `/usr/share/watertown/vendor` (these are inherited from the rename, migration
  plan items A/D, not invented here).
- The CLI binary stays `/usr/bin/pond`, the `pond-deb` artifact name keeps its
  `pond` element, and every genuine `duckdb-*` asset is untouched.

Section 1 below describes the current (pre-rename) state, so it still uses the
present `duckpond` names. This document lives under `docs/` and is in the
rename's scope, so its remaining `duckpond` brand tokens are swept to
`watertown` along with the rest of the active tree when the rename lands.

---

## 1. Problem / current state

Two update paths exist today; only the container one is automated.

### Container ponds (water / noyo / septic / site) -- fully automated

- `.github/workflows/rust-ci.yml` builds and pushes multi-arch podman images to
  ghcr on every push to `main`:
  `ghcr.io/jmacd/duckpond/duckpond:{latest,sha-<sha>}-<arch>`.
- On the hosts, `run.sh` runs `pond.sh <instance> --pull-image` once per timer
  tick, so each host auto-pulls the newest mutable tag.
- `.github/workflows/promote.yml` (manual `workflow_dispatch`) `crane cp`'s
  `latest-<arch> -> prod-<arch>` by digest (no rebuild) plus a dated
  `prod-<stamp>-<arch>` rollback handle, then cosign-signs. `pond.sh` selects
  `latest-` for `*-staging` instances and `prod-` otherwise.

### Selfmon pond (watershop-selfmon) -- not automated

- Runs a **natively-installed** `/usr/bin/pond` from a `.deb` (no podman).
- The `.deb` is built **only** when a human runs `tools/build-on-watershop.sh`:
  push branch, ssh to watershop, `git reset --hard <sha>` in `~/src/duckpond`,
  `make vendor`, `cargo deb -p cmd`, `dpkg -i`. There is no CI involvement, so
  the selfmon binary drifts stale until someone runs the deploy script.
- `tools/deploy-watershop.sh` wraps that build with a `terraform apply`;
  `install-duckpond.sh` installs the newest local `.deb`.

### Findings that motivate this work

1. `RELEASING.md` currently claims CI "create[s] .deb package" and publishes
   `pond_<version>_amd64.deb` as a GitHub Actions artifact. This is **not true**
   -- `rust-ci.yml` has no `cargo deb` step. The doc is aspirational and must be
   corrected as part of this work.
2. The build primitive already exists. `crates/cmd/Cargo.toml` has a working
   `[package.metadata.deb]` section that ships `/usr/bin/pond` plus the sitegen
   vendor blobs to `/usr/share/duckpond/vendor/` (the path
   `sitegen::factory::find_vendor_dir()` searches at runtime), and `make vendor`
   is already wired. Only CI packaging and a box-side auto-update are missing.

### Environmental facts that shaped the design

- The `jmacd/duckpond` repository is **public**, and the hosts pull ghcr images
  with no login (public packages). An `oras` pull of a public artifact likewise
  needs no credentials on the box.
- The selfmon timer fires every 1 minute (`OnUnitActiveSec=1min`), which is far
  too frequent for a registry pull -- so updates use a **separate hourly timer**.
- `watershop-selfmon` has no `-staging`/`-prod` suffix, so its channel cannot be
  derived from the instance name the way container channels are. It needs an
  explicit `DEB_CHANNEL` environment variable.
- `make vendor` requires Node.js >= 18 and npm, plus optional `brotli`/`gzip`
  for the pre-compressed `.br`/`.gz` variants.

---

## 2. Chosen design

Publish the deb as its own ghcr OCI artifact package and let the box pull it on
a timer, mirroring the container image flow end to end (build -> latest -> pull;
promote latest -> prod).

### Locked defaults

- **Separate package** `ghcr.io/jmacd/duckpond/pond-deb`, so the deb tags do not
  clutter the image package's tag listing. It reuses the image tag scheme within
  its own namespace.
- **Build both arches** (amd64 + arm64) for parity, even though only arm64
  (watershop) consumes it today. This lets `promote.yml` loop both arches
  identically to the image promotion.
- **Selfmon default channel `latest`** (it is experimental and tracks the
  bleeding edge), settable to `prod`.
- **Hourly update cadence**, decoupled from the 1-minute selfmon tick.

---

## 3. Work items

### Repo A -- duckpond (CI)

**A1. New `build-deb` job in `.github/workflows/rust-ci.yml`.**

- `needs: check`; matrix `{amd64: ubuntu-latest, arm64: ubuntu-24.04-arm}`
  (the same runners as `build-container`).
- Gate like `build-container`: always on `main`/`workflow_dispatch`; on pull
  requests only when the `build-container` label is present.
- Steps: checkout; setup Rust (`dtolnay/rust-toolchain@stable`); setup Node 18;
  `apt-get install -y brotli`; `cargo install cargo-deb`; install `oras` from a
  pinned release tarball; `make vendor`; `cargo deb -p cmd`.
- Tags on `ghcr.io/jmacd/duckpond/pond-deb`:
  - `main` / `workflow_dispatch`: `latest-<arch>` plus `sha-<short>-<arch>`.
  - `pull_request`: `pr-<n>-<arch>`.
- Publish with `oras push <pkg>:<tag> target/debian/duckpond_*_<arch>.deb`
  using a Debian media type (e.g. `application/vnd.debian.binary-package`).
  Log in to ghcr with `GITHUB_TOKEN` (`packages: write`), as the image job does.
- `cosign sign` the pushed artifact (`id-token: write`) for parity with image
  signing.

**A2. Extend `.github/workflows/promote.yml`.**

- After the existing image-promote loop, add a `pond-deb` loop: for each arch,
  `crane cp <pkg>:latest-<arch> <pkg>:prod-<arch>` and
  `<pkg>:prod-<stamp>-<arch>`, then `cosign sign` both.
- `crane cp` operates on arbitrary OCI artifacts (including oras-pushed ones)
  and preserves the digest, so promotion is a retag, not a rebuild.

### Repo B -- caspar.water (box-side auto-update)

**B1. `config/scripts/update-selfmon.sh` (new).**

- Resolve `ARCH` (from `uname -m`) and `DEB_CHANNEL` (from the instance env,
  default `latest`).
- `oras pull ghcr.io/jmacd/duckpond/pond-deb:<channel>-<arch>` into a temp dir
  (anonymous; public package).
- Compare `dpkg-deb -f <deb> Version` against
  `dpkg-query -W -f='${Version}' duckpond`; run `sudo dpkg -i <deb>` only if the
  pulled version is strictly newer, then log `pond --version`. Otherwise no-op.
- Idempotent and non-fatal, mirroring the "warn, do not abort" style of
  `run-selfmon.sh`.

**B2. systemd units.**

- `config/systemd/pond-selfmon-update@.service`: `Type=oneshot`,
  `ExecStart=.../config/scripts/update-selfmon.sh %i`, `Environment=HOME=...`.
- Timer `pond-selfmon-update@watershop-selfmon.timer`: `OnBootSec=2min`,
  `OnUnitActiveSec=1h`.
- Generate, install, and enable in terraform alongside the existing selfmon
  units (`watershop.tf` `timer_files` + `null_resource.watershop` provisioners).

**B3. terraform (`terraform/station/watershop/`).**

- Install the `oras` binary on watershop (remote-exec: download the pinned arm64
  release tarball with checksum verification to `/usr/local/bin/oras`).
- Add `DEB_CHANNEL=latest` to the selfmon instance env (produced by the
  `env_files` resource).
- Install `pond-selfmon-update@.service` and enable the new timer.

**B4. Docs / cleanup.**

- Fix `RELEASING.md` to describe the real flow (ghcr OCI deb artifact, oras
  pull, hourly timer, promotion via `promote.yml`).
- Keep `tools/build-on-watershop.sh` documented as a **dev-only fallback** for
  fast local iteration without waiting for CI; routine updates now come from CI
  plus the hourly timer.

---

## 4. Recommended sequencing

1. Land Repo A (A1 + A2) first and verify that `pond-deb:latest-arm64` publishes
   and that `promote.yml` produces `prod-arm64`. This has no production impact.
2. Land Repo B (B1-B4) once the artifact exists to pull.

---

## 5. Open questions

These must be resolved before or during implementation. Each notes the current
leaning so the decision is explicit rather than implicit.

1. **Artifact filename vs. version discovery.** `cargo deb` emits
   `duckpond_<version>_<arch>.deb`, but the box pulls by a mutable channel tag
   and does not know the version in advance. Confirm `oras pull` preserves the
   original filename (then the box reads the version via `dpkg-deb -f`), or push
   under a stable artifact filename and carry the version only as an OCI
   annotation. *Leaning: preserve the real filename; read version with
   `dpkg-deb -f`.*

2. **`dpkg -i` while a tick is running.** The 1-minute tick may be executing
   `/usr/bin/pond` when the hourly updater replaces it. On Linux this is safe
   (the running process keeps its open inode), and the hourly cadence makes
   overlap rare. Decide whether that is acceptable or whether the updater and
   the tick should share an flock. *Leaning: accept the open-inode semantics;
   no lock initially. Revisit if an in-flight upgrade ever corrupts a tick.*

3. **Signature verification on the box.** Images are cosign-signed but the hosts
   do not verify signatures today. Deciding whether `update-selfmon.sh` should
   `cosign verify` the artifact before `dpkg -i` is new scope. *Leaning: defer
   verification to keep parity with the current image flow; add later as a
   fleet-wide policy.*

4. **`oras` install and pinning on watershop.** Choose the install method and
   pin a version + checksum for the release tarball to avoid supply-chain drift.
   *Leaning: pinned tarball with checksum via terraform remote-exec.*

5. **amd64 consumer.** Only arm64 (watershop) consumes the deb today. Building
   amd64 is cheap parity but currently has no consumer. Decide whether to build
   both arches now or arm64-only until an amd64 host appears. *Leaning: build
   both for promotion-loop symmetry; trivially droppable.*

6. **Default channel for selfmon.** `latest` (bleeding edge, like staging) vs.
   `prod` (promoted). The `DEB_CHANNEL` env makes this a per-instance setting.
   *Leaning: default `latest`; the whole point of selfmon is to exercise the
   newest code first.*

7. **Separate package vs. shared package with a `deb-` tag prefix.** A separate
   `pond-deb` package keeps image tag listings clean; a shared package with
   prefixed tags keeps everything under one name. *Leaning: separate package.*

8. **Promotion coupling.** Should the deb be promoted in the same
   `promote.yml` run as the images (guaranteeing selfmon-prod and
   container-prod always move together), or via an independent dispatch input?
   *Leaning: same run/same loop for lockstep promotion; add an opt-out input
   only if a need arises.*

9. **Rename cutover dependency.** This whole pipeline is gated on the watertown
   rename completing first (repo rename, image package cutover to
   `ghcr.io/jmacd/watertown/watertown`, Debian package name + `/usr/share/`
   install-path flip, and the lockstep caspar.water deployment update). Do not
   author or land any of the work items in Section 3 until that cutover has
   soaked. *Leaning: hard dependency; build the deb pipeline only after the
   rename is live and verified on staging.*

---

## 6. Non-goals (this iteration)

- A full apt repository (reprepro / aptly) -- rejected as too heavy.
- GPG-signed apt metadata / an `apt upgrade` UX.
- Any change to the production container ponds, which remain image-based.
