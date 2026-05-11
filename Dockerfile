# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: Build the operator binary
# =============================================================================
FROM golang:1.26-alpine AS builder

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
COPY cmd/operator/ cmd/operator/
COPY internal/ internal/

# Build the operator binary with optimizations
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build \
    -trimpath \
    -ldflags="-s -w \
        -X main.version=${VERSION} \
        -X main.commit=${COMMIT} \
        -X main.buildDate=${BUILD_DATE}" \
    -o /workspace/bin/cloudberry-operator \
    ./cmd/operator/

# =============================================================================
# Stage 2: Runtime image
# =============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

# OCI labels
LABEL org.opencontainers.image.title="cloudberry-operator" \
      org.opencontainers.image.description="Kubernetes operator for Cloudberry Database clusters" \
      org.opencontainers.image.vendor="Cloudberry Contributors" \
      org.opencontainers.image.source="https://github.com/cloudberry-contrib/cloudberry-k8s" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /

# Copy the binary from builder
COPY --from=builder /workspace/bin/cloudberry-operator /cloudberry-operator

# Use non-root user (65532 is the nonroot user in distroless)
USER 65532:65532

EXPOSE 8080 8081 8443 9443

ENTRYPOINT ["/cloudberry-operator"]
