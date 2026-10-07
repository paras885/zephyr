FROM golang:1.27.1-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/zephyr-server ./cmd/zephyr-server

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S -g 10001 zephyr \
    && adduser -S -D -H -u 10001 -G zephyr zephyr \
    && mkdir -p /app/workflows \
    && chown -R 10001:10001 /app

WORKDIR /app
COPY --chown=10001:10001 examples/quickstart/ /app/workflows/

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/app/zephyr-server"]

FROM runtime AS kind-runtime
COPY --chown=10001:10001 .kind-build/zephyr-server /app/zephyr-server

FROM runtime AS production
COPY --from=build --chown=10001:10001 /out/zephyr-server /app/zephyr-server