FROM ghcr.io/astral-sh/uv:0.10.12 AS uv
FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

COPY --from=uv /uv /uvx /usr/local/bin/

# Install the official prebuilt just binary for the container's architecture.
RUN curl --fail --silent --show-error --location https://just.systems/install.sh -o /tmp/install-just.sh \
    && bash /tmp/install-just.sh --tag 1.40.0 --to /usr/local/bin \
    && rm /tmp/install-just.sh

ENV UV_LINK_MODE=copy
