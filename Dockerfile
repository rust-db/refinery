FROM rust:1 AS builder
WORKDIR app

COPY . .
RUN cargo build -p refinery_cli --release --all-features


FROM debian:bullseye-slim AS runtime
COPY --from=builder /app/target/release/refinery /usr/local/bin
ENTRYPOINT ["/usr/local/bin/refinery"]
