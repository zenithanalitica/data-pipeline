FROM rust:1.87.0-alpine AS builder
RUN apk add --no-cache musl-dev # Needed to compile some dependencies
WORKDIR /app
COPY . .
RUN cargo build --release

FROM alpine
COPY --from=builder /app/target/release/data-pipeline /
ENTRYPOINT ["./data-pipeline"]
