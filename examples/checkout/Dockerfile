FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY . .
RUN go run ./cmd/zephyr workflow artifacts --file examples/checkout/workflows/checkout.zephyr --output examples/checkout/generated --force
WORKDIR /src/examples/checkout
RUN CGO_ENABLED=0 go build -trimpath -o /out/checkout ./cmd/checkout

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 consumer \
    && adduser -S -D -H -u 10001 -G consumer consumer
COPY --from=build /out/checkout /app/checkout
USER 10001:10001
ENTRYPOINT ["/app/checkout"]
