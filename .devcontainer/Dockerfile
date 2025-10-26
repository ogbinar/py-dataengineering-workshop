FROM mcr.microsoft.com/devcontainers/python:3.11
RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates curl jq groff less unzip && \
  rm -rf /var/lib/apt/lists/*

# ---- uv (download with -k but verify with SHA) ----
ARG TARGETARCH
ARG UV_VERSION=0.9.0
ARG UV_SHA256

RUN set -euo pipefail; \
  case "$TARGETARCH" in \
  amd64) TRIPLE="x86_64-unknown-linux-gnu" ;; \
  arm64) TRIPLE="aarch64-unknown-linux-gnu" ;; \
  *) echo "Unsupported arch: $TARGETARCH" && exit 1 ;; \
  esac; \
  UV_URL="https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${TRIPLE}.tar.gz"; \
  echo "Fetching: ${UV_URL}"; \
  curl -fsSLk "$UV_URL" -o /tmp/uv.tar.gz; \
  if [ -n "${UV_SHA256:-}" ]; then echo "${UV_SHA256}  /tmp/uv.tar.gz" | sha256sum -c -; fi; \
  mkdir -p /tmp/uv && tar -xzf /tmp/uv.tar.gz -C /tmp/uv; \
  install -m 0755 "$(find /tmp/uv -type f -name uv -print -quit)" /usr/local/bin/uv; \
  rm -rf /tmp/uv /tmp/uv.tar.gz; \
  uv --version