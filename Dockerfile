FROM rust:slim-bookworm AS build
COPY . .
RUN cd mtop && cargo build --release

FROM debian:bookworm-slim
COPY --from=build target/release/mtop /usr/local/bin/
COPY --from=build target/release/mc /usr/local/bin/
COPY --from=build target/release/dns /usr/local/bin/
CMD ["mtop", "--help"]
