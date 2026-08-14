.PHONY: generate compat-server

FUJIN_SERVER_ROOT ?= ../fujin

generate:
	@echo "==> Generating gRPC messages"
	@cd grpc/v1/proto && protoc --go_out=. --go_opt=paths=source_relative fujin.proto

compat-server:
	@FUJIN_SERVER_ROOT="$(FUJIN_SERVER_ROOT)" go test -count=1 -run '^TestServerCompatibility$$' .
