ARG VERSION=0.0.1

## FROM BUILDER
FROM golang:1.23-alpine AS builder

ARG VERSION

RUN apk update && apk upgrade --no-cache
RUN apk add bash

WORKDIR /src

# COPY files
COPY ./src .
COPY ./config/ ./config
COPY ./docker-entrypoint.sh .

# Build Golang
RUN go mod tidy && env GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -v -a -ldflags="-s -w" -o datagen

## RUNNER
FROM alpine

RUN apk add --no-cache bash && mkdir logs app

COPY --from=builder /src/datagen ./app
COPY --from=builder /src/docker-entrypoint.sh ./
COPY --from=builder /src/config ./config

## dir permissions
RUN chgrp -R 0 /config /app /logs ; \
    chmod -R g+rwX /config /app /logs

## Default Setting
ENV BOOTSTRAP__SERVER=localhost:9092
ENV TOPIC_PARTITION=3
ENV TOPIC_REPLICA__FACTOR=1
ENV DATAGEN_GO__ROUTINE=1

ENTRYPOINT ["./docker-entrypoint.sh"]
