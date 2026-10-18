# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin raft-node

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 raft && mkdir /data && chown raft /data
COPY --from=build /src/target/release/raft-node /usr/local/bin/raft-node
USER raft
VOLUME ["/data"]
EXPOSE 7000
ENTRYPOINT ["raft-node"]
