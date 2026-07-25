# Stage 1: Build the statically linked Go binary
FROM golang:1.22-alpine AS builder

# Install git and ca-certificates required by go mod for VCS dependencies
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy dependency mappings and cache downloads
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /out/node ./cmd/node

# Stage 2: Runtime environment
FROM alpine:3.19

# Install ca-certificates and curl for health check utility
RUN apk add --no-cache ca-certificates curl

# Setup non-root execution context for security with static UID/GID 1000
RUN addgroup -g 1000 raft && adduser -u 1000 -S -G raft raft

# Pre-create resource paths and assign ownership to non-root user
RUN mkdir -p /data /certs && chown -R raft:raft /data /certs

USER raft
WORKDIR /app

# Copy compiled binary from build stage
COPY --from=builder /out/node /app/node

# Expose internal API and peer communication ports
EXPOSE 8080 9000

# Health check setup hitting our local HTTP API endpoint
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/node"]
