.PHONY: generate
generate:
	@echo "==> Generating gRPC code"
	@protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/v1/fujin_v1.proto
