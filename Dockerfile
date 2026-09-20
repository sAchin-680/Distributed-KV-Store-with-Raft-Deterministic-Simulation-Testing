# syntax=docker/dockerfile:1

# Build ----------------------------------------------------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Dependencies first, so a source-only change does not re-download them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

# CGO off so the result runs on any base image, including a scratch one.
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w \
      -X github.com/sAchin-680/raftkv/internal/buildinfo.Version=${VERSION} \
      -X github.com/sAchin-680/raftkv/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/sAchin-680/raftkv/internal/buildinfo.Date=${DATE}" \
    -o /out/raftd ./cmd/raftd && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/kvctl ./cmd/kvctl

# Runtime --------------------------------------------------------------------
# What actually ships: the binaries, a CA bundle, and nothing else. No shell, no
# package manager, no network tools — a smaller image is mostly a smaller
# attack surface.
FROM alpine:3.21 AS runtime

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 10001 raft && \
    mkdir -p /data && chown raft:raft /data

COPY --from=builder /out/raftd /usr/local/bin/raftd
COPY --from=builder /out/kvctl /usr/local/bin/kvctl

USER raft
VOLUME /data
EXPOSE 9001 8001

ENTRYPOINT ["/usr/local/bin/raftd"]

# Chaos ----------------------------------------------------------------------
# The same binaries plus the tools needed to damage the network from inside the
# container. A separate target rather than a flag, so the network-manipulation
# tools cannot end up in a production image by accident.
#
# Runs as root because tc and iptables need CAP_NET_ADMIN, which is exactly why
# this image is not the one to deploy.
FROM runtime AS chaos

USER root
RUN apk add --no-cache iproute2 iptables
USER root
