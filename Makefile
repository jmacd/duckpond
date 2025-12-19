.PHONY: help install-reuse add-headers check-headers lint-reuse

help:
	@echo "Available targets:"
	@echo "  install-reuse    - Install the REUSE tool via pip"
	@echo "  add-headers      - Add SPDX headers to all Rust files"
	@echo "  check-headers    - Check SPDX compliance"
	@echo "  lint-reuse       - Same as check-headers"

install-reuse:
	@echo "Installing REUSE tool..."
	brew install reuse

add-headers: install-reuse
	@echo "Adding SPDX headers to Rust files..."
	reuse annotate --copyright="Caspar Water Company" \
		--license="Apache-2.0" \
		--skip-existing \
		--recursive \
		crates

check-headers: install-reuse
	@echo "Checking SPDX compliance..."
	reuse lint

lint-reuse: check-headers
