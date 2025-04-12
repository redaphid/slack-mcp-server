FROM golang:1.23 AS build

ENV CGO_ENABLED=0
ENV GOTOOLCHAIN=local
ENV GOCACHE=/go/pkg/mod

RUN apt-get update  \
  && apt-get install -y --no-install-recommends net-tools curl \

WORKDIR /app

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . /app/mcp-server

RUN --mount=type=cache,target=/go/pkg/mod \
    go build -ldflags="-s -w" -o /go/bin/mcp-server ./cmd/

FROM build AS dev

RUN --mount=type=cache,target=/go/pkg/mod \
    go install github.com/go-delve/delve/cmd/dlv@v1.23.1 && cp /go/bin/dlv /dlv

WORKDIR /app/mcp-server

EXPOSE 8080

CMD ["mcp-server"]

FROM alpine:3.18 AS production

RUN apk add --no-cache ca-certificates tor net-tools curl

COPY --from=build /go/bin/mcp-server /usr/local/bin/pummcp-serverpe

WORKDIR /app

EXPOSE 8080

CMD ["mcp-server"]
