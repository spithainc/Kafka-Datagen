#!/bin/bash

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
protoc -I. -I$GOPATH/src --go_out=. message/protobuf/*.proto