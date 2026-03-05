FROM golang:1.23-bookworm AS builder

WORKDIR /build

# Private module access
ARG GITHUB_TOKEN
ENV GOPRIVATE=github.com/bytefreezer/*
RUN if [ -n "$GITHUB_TOKEN" ]; then \
    git config --global url."https://x-access-token:${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG BUILD_TIME=unknown

RUN go build -ldflags "-X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" -o bytefreezer-connector .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates wget && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/bytefreezer-connector .
COPY config.yaml .

EXPOSE 8090

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD wget -q --spider http://127.0.0.1:8090/api/v1/health || exit 1

ENTRYPOINT ["./bytefreezer-connector"]
