FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/platform ./cmd/platform \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fakesource ./cmd/fakesource

FROM alpine:3.22
RUN addgroup -S streamforge && adduser -S -G streamforge streamforge
COPY --from=build /out/platform /usr/local/bin/platform
COPY --from=build /out/fakesource /usr/local/bin/fakesource
USER streamforge
ENTRYPOINT ["platform"]
