#!/bin/bash
# Detect the container runtime (docker or podman) and compose tool.
#
# Source this file from other scripts:
#   source "$(dirname "${BASH_SOURCE[0]}")/container-runtime.sh"
#
# Provides:
#   CONTAINER_RT   - "docker" or "podman"
#   COMPOSE_CMD    - compose invocation (array), e.g. ("docker" "compose") or
#                    ("podman" "compose"), or empty if unavailable
#
# Users can override by setting CONTAINER_RT before sourcing.

if [[ -z "${CONTAINER_RT}" ]]; then
    if command -v docker &>/dev/null; then
        CONTAINER_RT=docker
    elif command -v podman &>/dev/null; then
        CONTAINER_RT=podman
    else
        echo "ERROR: No container runtime found. Install docker or podman." >&2
        exit 1
    fi
fi

# Detect compose support
COMPOSE_CMD=()
if [[ "${CONTAINER_RT}" == "docker" ]]; then
    # Modern Docker bundles "docker compose" as a plugin
    if docker compose version &>/dev/null 2>&1; then
        COMPOSE_CMD=("docker" "compose")
    elif command -v docker-compose &>/dev/null; then
        COMPOSE_CMD=("docker-compose")
    fi
elif [[ "${CONTAINER_RT}" == "podman" ]]; then
    if podman compose version &>/dev/null 2>&1; then
        COMPOSE_CMD=("podman" "compose")
    elif command -v podman-compose &>/dev/null; then
        COMPOSE_CMD=("podman-compose")
    elif command -v docker-compose &>/dev/null; then
        COMPOSE_CMD=("docker-compose")
    fi
fi

export CONTAINER_RT
