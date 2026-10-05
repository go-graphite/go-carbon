GO ?= go

GOLANGCI_LINT_VERSION ?= v2.2.2
TOOLS_DIR := $(CURDIR)/.tools
GOLANGCI_LINT := $(TOOLS_DIR)/golangci-lint

.PHONY: test
test:
	$(GO) test -race $(shell $(GO) list ./...)
	cd godeltaprof && $(GO) test -race ./...
	cd godeltaprof/compat && $(GO) test -race ./...

.PHONY: go/mod
go/mod:
	GO111MODULE=on go mod download
	GO111MODULE=on go mod tidy
	cd godeltaprof/compat/ && GO111MODULE=on go mod download
	cd godeltaprof/compat/ && GO111MODULE=on go mod tidy
	cd godeltaprof/ && GO111MODULE=on go mod download
	cd godeltaprof/ && GO111MODULE=on go mod tidy


.PHONY: k6/test
k6/test:
	cd x/k6 && $(GO) test -race ./...

.PHONY: k6/go/mod
k6/go/mod:
	cd x/k6 && GO111MODULE=on go mod download
	cd x/k6 && GO111MODULE=on go mod tidy

.PHONY: install-lint
install-lint:
	@ mkdir -p $(TOOLS_DIR)
	@ GOBIN=$(TOOLS_DIR) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: lint
lint: install-lint
	$(GOLANGCI_LINT) run
	cd godeltaprof && $(GOLANGCI_LINT) run
	cd godeltaprof/compat && $(GOLANGCI_LINT) run


