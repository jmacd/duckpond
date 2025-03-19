# See https://github.com/LukeMathWalker/cargo-chef
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo +nightly chef prepare --recipe-path recipe.json

FROM chef AS builder 
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer!
RUN cargo +nightly chef cook --profile=dev --recipe-path recipe.json

# Build application
COPY . .
RUN cargo +nightly build --profile=dev --bin duckpond

# We do not need the Rust toolchain to run the binary!
FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Update the system's trusted certificates
RUN update-ca-certificates

WORKDIR /app
COPY --from=builder /app/target/debug/duckpond /usr/local/bin
ENTRYPOINT ["/usr/local/bin/duckpond"]
