# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/whitelists ./cmd/whitelists

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && addgroup -S whitelists && adduser -S -G whitelists whitelists
COPY --from=build /out/whitelists /usr/local/bin/whitelists
USER whitelists:whitelists

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD sh -c 'P="$${HTTP_PORT:-$${HTTP_ADDR##*:}}"; P="$${P:-$${http_adr##*:}}"; P="$${P:-$${HTTP_ADR##*:}}"; P="$${P:-$${http_addr##*:}}"; P="$${P:-8080}"; wget -qO- --header="X-API-Key: $$API_KEY" "http://127.0.0.1:$$P/healthz"' >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/whitelists"]
