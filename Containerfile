FROM golang:1.27 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pipe-server ./cmd/pipe-server

FROM alpine:3.21

RUN addgroup -S aipipe && adduser -S -G aipipe aipipe \
    && mkdir -p /data \
    && chown -R aipipe:aipipe /data

COPY --from=build /out/pipe-server /usr/local/bin/pipe-server

USER aipipe
VOLUME ["/data"]
EXPOSE 8080
ENV AIPIPE_ADDR=:8080
ENV AIPIPE_DB=/data/pipe.db

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O - http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/pipe-server"]
