FROM rust:1.95 as builder
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY iron-carrier/Cargo.toml iron-carrier/Cargo.toml
COPY iron-carrier-macros/Cargo.toml iron-carrier-macros/Cargo.toml

# Build dependencies against stub sources so this layer is cached across code changes.
RUN mkdir -p iron-carrier/src iron-carrier-macros/src \
    && echo "fn main() {}" > iron-carrier/src/main.rs \
    && touch iron-carrier-macros/src/lib.rs \
    && cargo build --release --package iron-carrier \
    && rm -rf iron-carrier/src iron-carrier-macros/src

COPY iron-carrier/ iron-carrier/
COPY iron-carrier-macros/ iron-carrier-macros/

# Force cargo to see the real sources as newer than the stub build's fingerprints.
RUN find iron-carrier/src iron-carrier-macros/src -type f -exec touch {} + \
    && cargo build --release --package iron-carrier

FROM debian:bookworm-slim AS runtime
WORKDIR /app
RUN apt-get update && apt-get install -y sqlite3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/iron-carrier /usr/local/bin/iron-carrier
ENTRYPOINT ["/usr/local/bin/iron-carrier"]
