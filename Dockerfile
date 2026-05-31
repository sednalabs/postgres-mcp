# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS builder
WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /usr/sbin/nologin mcp
WORKDIR /home/mcp

COPY --from=builder /workspace/target/release/postgres-mcp /usr/local/bin/postgres-mcp

USER mcp
ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/postgres-mcp"]
