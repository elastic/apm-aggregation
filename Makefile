.DEFAULT_GOAL := all
all: test

fmt:
	@go tool github.com/elastic/go-licenser -license=Elasticv2 .
	@go tool golang.org/x/tools/cmd/goimports -local github.com/elastic/ -w .

lint:
	go mod tidy -diff
	go tool honnef.co/go/tools/cmd/staticcheck -checks=all ./...

protolint:
	docker run --volume "$(PWD):/workspace" --workdir /workspace bufbuild/buf lint proto
	docker run --volume "$(PWD):/workspace" --workdir /workspace bufbuild/buf breaking proto --against https://github.com/elastic/apm-aggregation.git#branch=main,subdir=proto

.PHONY: clean
clean:
	rm -fr bin build

.PHONY: test
test: go.mod
	go test -v -race ./...

##############################################################################
# Protobuf generation
##############################################################################

GITROOT ?= $(shell git rev-parse --show-toplevel)
GOOSBUILD:=$(GITROOT)/build/$(shell go env GOOS)
PROTOC=$(GOOSBUILD)/protoc/bin/protoc
PROTOC_GEN_GO_VTPROTO=$(GOOSBUILD)/protoc-gen-go-vtproto
PROTOC_GEN_GO=$(GOOSBUILD)/protoc-gen-go

$(PROTOC):
	@./tools/install-protoc.sh

$(PROTOC_GEN_GO_VTPROTO):
	GOBIN=$(GOOSBUILD) go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto

$(PROTOC_GEN_GO):
	GOBIN=$(GOOSBUILD) go install google.golang.org/protobuf/cmd/protoc-gen-go

PROTOC_OUT?=.

.PHONY: gen-proto
gen-proto: $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_VTPROTO) $(PROTOC)
	$(eval STRUCTS := $(shell grep '^message' proto/*.proto | cut -d ' ' -f2))
	$(PROTOC) -I . --go_out=$(PROTOC_OUT) --plugin protoc-gen-go="$(PROTOC_GEN_GO)" \
	--go-vtproto_out=$(PROTOC_OUT) --plugin protoc-gen-go-vtproto="$(PROTOC_GEN_GO_VTPROTO)" \
	--go-vtproto_opt=features=marshal+unmarshal+size+clone \
	$(wildcard proto/*.proto)
	go generate ./aggregators/internal/protohash
	$(MAKE) fmt
