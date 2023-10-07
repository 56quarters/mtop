FROM rust:slim-bookworm AS BUILD
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=BUILD target/release/mtop /usr/local/bin/
COPY --from=BUILD target/release/mc /usr/local/bin/
CMD ["mtop", "--help"]
