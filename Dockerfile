# syntax=docker/dockerfile:1
# Rosace lobby server image (multi-stage, ~30 MB).
#   docker build -t rosace-lobby .
#   docker run -p 8080:8080 -p 7777:7777/udp -p 7778:7778/udp rosace-lobby
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p lobby-server \
    && cp target/release/lobby-server /lobby-server

# distroless: glibc + libgcc only, no shell, runs as uid 65532.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /lobby-server /usr/local/bin/lobby-server
ENV LOBBY_HTTP_ADDR=0.0.0.0:8080 \
    LOBBY_RENDEZVOUS_ADDR=0.0.0.0:7777 \
    LOBBY_RELAY_ADDR=0.0.0.0:7778 \
    LOBBY_LOG=info
EXPOSE 8080/tcp 7777/udp 7778/udp
# The binary probes its own /health (no curl in the image).
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD ["/usr/local/bin/lobby-server", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/lobby-server"]
