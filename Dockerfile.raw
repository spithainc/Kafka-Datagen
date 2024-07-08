ARG VERSION=0.0.1

FROM golang:1.21-alpine as builder

ARG VERSION

RUN apk add upx

WORKDIR /src

COPY . .

RUN go mod tidy && env GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -v -a -ldflags="-s -w" -o datagen
RUN upx --best --lzma datagen

FROM scratch as exporter

COPY --from=builder /src/datagen ./
