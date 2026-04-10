# Vendor stage — download JS/WASM dependencies for sitegen (DuckDB-WASM,
# Observable Plot, D3).  Runs in parallel with the Rust build.
FROM node:18-slim AS vendor
WORKDIR /vendor
COPY crates/sitegen/vendor/download.sh .
RUN bash download.sh

# Build stage
FROM rust:1.88 AS builder
WORKDIR /app

# Copy source
COPY . .

# Build the pond binary
RUN cargo build --release --bin pond

# Runtime stage
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/pond /usr/local/bin/pond
COPY --from=vendor /vendor/dist/ /usr/local/share/duckpond/vendor/

ENTRYPOINT ["/usr/local/bin/pond"]
