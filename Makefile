GO ?= go
GOPATH := $(CURDIR)/_vendor

all: build

build:
	$(GO) build

