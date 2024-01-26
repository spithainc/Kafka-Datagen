ARG VERSION=0.0.1

## FROM BUILDER
FROM golang:1.21-alpine as builder

ARG VERSION

RUN apk add upx bash

WORKDIR /src

# COPY files
COPY ./src .
COPY ./config/ ./config
COPY ./docker-entrypoint.sh .

# Build Golang
RUN go mod tidy && env GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -v -a -ldflags="-s -w" -o datagen
RUN upx --best --lzma datagen

## RUNNER
FROM alpine

RUN apk add --no-cache bash && mkdir logs app

COPY --from=builder /src/datagen ./app
COPY --from=builder /src/docker-entrypoint.sh ./
COPY --from=builder /src/config ./config

## Default Setting
ENV BOOTSTRAP_SERVER=localhost:9092
ENV TOPIC_PARTITION=3
ENV TOPIC_REPLICA_FACTOR=1
ENV DATAGEN_GO_ROUTINE=1

ENTRYPOINT ["./docker-entrypoint.sh"]
