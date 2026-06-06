# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: Build the cloudberry-ctl binary
# =============================================================================
FROM golang:1.26.4-alpine AS builder

# Install build dependencies (sorted alphanumerically)
RUN apk add --no-cache \
    ca-certificates \
    git \
    tzdata

WORKDIR /workspace

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY api/ api/
COPY cmd/cloudberry-ctl/ cmd/cloudberry-ctl/
COPY internal/ internal/

# Build the cloudberry-ctl binary with optimizations
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build \
    -trimpath \
    -ldflags="-s -w \
        -X main.version=${VERSION} \
        -X main.commit=${COMMIT} \
        -X main.buildDate=${BUILD_DATE}" \
    -o /workspace/bin/cloudberry-ctl \
    ./cmd/cloudberry-ctl/

# =============================================================================
# Stage 2: Runtime image
# =============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

# OCI labels
LABEL org.opencontainers.image.title="cloudberry-ctl" \
      org.opencontainers.image.description="CLI utility for Cloudberry Database cluster management" \
      org.opencontainers.image.vendor="Cloudberry Contributors" \
      org.opencontainers.image.source="https://github.com/cloudberry-contrib/cloudberry-k8s" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /

# Copy the binary from builder
COPY --from=builder /workspace/bin/cloudberry-ctl /cloudberry-ctl

# Use non-root user (65532 is the nonroot user in distroless)
USER 65532:65532

ENTRYPOINT ["/cloudberry-ctl"]
