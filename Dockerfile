# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

FROM rust:1.57-alpine as build

RUN apk add build-base

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src/

RUN echo "fn main() {}" > src/main.rs
RUN cargo build --release

COPY . .

RUN cargo build --release

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM alpine as runtime

EXPOSE 25230

WORKDIR /app
COPY --from=build /app/target/release/iron-carrier /usr/bin
COPY tests/configs/ configs/

# RUN printf "[paths]\nsync = \"/app/sync\"\n\nlog_path = \"/app/log\"" > config.toml

RUN mkdir sync/
RUN mkdir log/


CMD ["iron-carrier", "-vvvv", "/app/config.toml"]
