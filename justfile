#!/usr/bin/env just --justfile

# Load environment variables from `.env` file.
set dotenv-load

GOBUILD_COMMAND := GO + " build"

DIST_PATH := "dist"

GO := "go"

export CODE_RELEASE := `git rev-parse HEAD`

# print available targets
default:
    @just --list --justfile {{justfile()}}

# evaluate and print all just variables
evaluate:
    @just --evaluate

# format source code
format:
    gofmt -l -s -w .

# detect known vulnerabilities (requires https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck)
vulnerabilities:
    govulncheck ./...

# add missing module requirements for imported packages, removes requirements that aren't used anymore
tidy:
    go mod tidy

# show dependencies
deps:
    go mod graph

# Run wal-cake
start:
    go run ./{{DIST_PATH}}/cake

#build the main worker

build:
    rm -rf ./dist
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 {{GOBUILD_COMMAND}} -ldflags="-w -s" -o ./dist/cake ./cmd/cake/main.go

#build docker image for this
build-image TAG:
    docker build -t wal-cake:"{{TAG}}" .
