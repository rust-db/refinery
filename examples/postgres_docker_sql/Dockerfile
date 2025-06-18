FROM rust:slim as builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src

RUN cargo build --release

FROM debian:stable-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl-dev \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/refinery-migrations-postgres /usr/local/bin/refinery-migrations-postgres

ENV DATABASE_URL="postgres://postgres:password@db:5432/postgres"

ENTRYPOINT ["refinery-migrations-postgres"]
