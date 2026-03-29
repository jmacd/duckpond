# DuckPond Makefile
#
# Quick reference:
#   make              - show this help
#   make build        - build the pond binary (debug)
#   make test         - run unit tests
#   make integration  - build test image + run integration tests
#   make vendor       - download JS vendor dependencies (one-time, needs network)
#
# The three routine tasks: build, test, integration.
# Everything else is for maintenance or CI.

.PHONY: help build release test clippy fmt check \
        vendor \
        test-image integration integration-all \
        check-headers add-headers \
        clean

# ── Default ──────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  Development"
	@echo "    make build          Build pond (debug)"
	@echo "    make release        Build pond (release)"
	@echo "    make test           Run unit tests (cargo test)"
	@echo "    make clippy         Run clippy lints"
	@echo "    make fmt            Check formatting"
	@echo "    make check          All of: fmt, clippy, test"
	@echo ""
	@echo "  Vendor (one-time, requires network)"
	@echo "    make vendor         Download JS deps (DuckDB-WASM, Plot, D3)"
	@echo ""
	@echo "  Integration tests (requires Docker)"
	@echo "    make test-image     Build the test Docker image"
	@echo "    make integration    Build image + run all integration tests"
	@echo ""
	@echo "  Maintenance"
	@echo "    make check-headers  Check SPDX license headers"
	@echo "    make add-headers    Add SPDX headers to Rust files"
	@echo "    make clean          Remove build artifacts"
	@echo ""

# ── Development ──────────────────────────────────────────────────────────

build:
	cargo build --bin pond

release:
	cargo build --release --bin pond

test:
	cargo test

clippy:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt --check

check: fmt clippy test

# ── Vendor dependencies ──────────────────────────────────────────────────

vendor:
	@echo "=== Downloading vendor dependencies ==="
	cd crates/sitegen/vendor && bash download.sh

# ── Integration tests ────────────────────────────────────────────────────

test-image:
	cd testsuite && bash build-image.sh

integration: test-image
	cd testsuite && bash run-all.sh --no-rebuild --skip-browser

integration-all: test-image
	cd testsuite && bash run-all.sh --no-rebuild

# ── License headers ──────────────────────────────────────────────────────

check-headers:
	reuse lint

add-headers:
	reuse annotate --copyright="Caspar Water Company" \
		--license="Apache-2.0" \
		--skip-existing \
		--skip-unrecognised \
		--recursive \
		crates

# ── Cleanup ──────────────────────────────────────────────────────────────

clean:
	cargo clean
	rm -rf crates/sitegen/vendor/dist crates/sitegen/vendor/.work
	rm -f testsuite/pond testsuite/duckpond-emergency
