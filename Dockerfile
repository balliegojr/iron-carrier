FROM rust:1.95 as builder
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY iron-carrier/ ./iron-carrier/
COPY iron-carrier-macros/ ./iron-carrier-macros/

RUN cargo install --path ./iron-carrier

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /usr/local/cargo/bin/iron-carrier /usr/local/bin/iron-carrier
ENTRYPOINT ["/usr/local/bin/iron-carrier"]
