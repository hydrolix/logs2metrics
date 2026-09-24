# Stage 1: Build stage
FROM golang:1.25 AS builder
WORKDIR /home/metrics/
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w \
      -X 'github.com/mercereau/hydrolix-metrics-go/internal/build.Version=${VERSION}' \
      -X 'github.com/mercereau/hydrolix-metrics-go/internal/build.Commit=${COMMIT}' \
      -X 'github.com/mercereau/hydrolix-metrics-go/internal/build.Date=${DATE}'" \
    -o hydrolix-collector .

# Stage 2: Runtime stage
FROM alpine:3.21
WORKDIR /root/
COPY --from=builder /home/metrics/hydrolix-collector .

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q -O- http://127.0.0.1:2112/healthz || exit 1

CMD ["./hydrolix-collector"]