NAME:=go-carbon
DESCRIPTION:="Golang implementation of Graphite/Carbon server"
MODULE:=github.com/go-graphite/go-carbon

SHELL := bash
.ONESHELL:

GO ?= go
export GOFLAGS +=  -mod=vendor
export GO111MODULE := on
TEMPDIR:=$(shell mktemp -d)

DEVEL ?= 0
ifeq ($(DEVEL), 0)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')
else
VERSION:=$(shell sh -c 'git describe --always --tags | sed -e "s/^v//i"')
endif

RPM_VERSION:=$(subst -,_,$(VERSION))
BUILD ?= $(shell git describe --abbrev=4 --dirty --always --tags)

SRCS:=$(shell find . -name '*.go')

all: $(NAME)

$(NAME):
	$(GO) build -mod vendor --ldflags '-X main.BuildVersion=$(BUILD)' $(MODULE)

build-linux: ## Build the binary for linux and amd64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o $(NAME)

debug:
	$(GO) build -mod vendor -ldflags=-compressdwarf=false -gcflags=all='-l -N' $(MODULE)

run-test:
	$(GO) $(COMMAND) ./...

test:
	make run-test COMMAND="test"
	make run-test COMMAND="vet"
	make run-test COMMAND="test -race"

gox-build: out/$(NAME)-linux-386 out/$(NAME)-linux-amd64 out/$(NAME)-linux-arm64

ARCH = 386 amd64 arm64
out/$(NAME)-linux-%: out $(SRCS)
	GOOS=linux GOARCH=$* $(GO) build -ldflags '-X main.BuildVersion=$(VERSION)' -o $@ $(MODULE)

out:
	mkdir -p out

clean:
	rm -f go-carbon build/* *deb *rpm

image:
	CGO_ENABLED=0 GOOS=linux $(MAKE) go-carbon
	docker build -t go-carbon .

package-tree:
	install -m 0755 -d out/root/lib/systemd/system
	install -m 0755 -d out/root/etc/$(NAME)
	install -m 0755 -d out/root/etc/logrotate.d
	install -m 0755 -d out/root/etc/init.d
	install -m 0755 -d out/root/etc/$(NAME)
	./out/$(NAME)-linux-amd64 -config-print-default > out/root/etc/$(NAME)/$(NAME).conf
	install -m 0644 deploy/$(NAME).service out/root/lib/systemd/system/$(NAME).service
	install -m 0644 deploy/storage-schemas.conf out/root/etc/$(NAME)/storage-schemas.conf
	install -m 0644 deploy/storage-aggregation.conf out/root/etc/$(NAME)/storage-aggregation.conf
	install -m 0644 deploy/$(NAME).logrotate out/root/etc/logrotate.d/$(NAME)
	install -m 0755 deploy/$(NAME).init out/root/etc/init.d/$(NAME)

nfpm-deb: gox-build package-tree
	$(MAKE) nfpm-build-deb ARCH=386
	$(MAKE) nfpm-build-deb ARCH=amd64
	$(MAKE) nfpm-build-deb ARCH=arm64

nfpm-rpm: gox-build package-tree
	$(MAKE) nfpm-build-rpm ARCH=386
	$(MAKE) nfpm-build-rpm ARCH=amd64
	$(MAKE) nfpm-build-rpm ARCH=arm64

nfpm-build-%: nfpm.yaml
	NAME=$(NAME) DESCRIPTION=$(DESCRIPTION) ARCH=$(ARCH) VERSION_STRING=$(VERSION) nfpm package --packager $*

packagecloud-push-rpm: $(wildcard $(NAME)-$(RPM_VERSION)-1.*.rpm)
	for pkg in $^; do
		package_cloud push $(REPO)/el/7 $${pkg} || true
		package_cloud push $(REPO)/el/8 $${pkg} || true
		package_cloud push $(REPO)/el/9 $${pkg} || true
	done

packagecloud-push-deb: $(wildcard $(NAME)_$(VERSION)_*.deb)
	for pkg in $^; do
		package_cloud push $(REPO)/ubuntu/bionic   $${pkg} || true
		package_cloud push $(REPO)/ubuntu/focal    $${pkg} || true
		package_cloud push $(REPO)/ubuntu/jammy    $${pkg} || true
		package_cloud push $(REPO)/ubuntu/noble    $${pkg} || true
		package_cloud push $(REPO)/debian/stretch  $${pkg} || true
		package_cloud push $(REPO)/debian/buster   $${pkg} || true
		package_cloud push $(REPO)/debian/bullseye $${pkg} || true
		package_cloud push $(REPO)/debian/bookworm $${pkg} || true
	done

packagecloud-push:
	@$(MAKE) packagecloud-push-rpm
	@$(MAKE) packagecloud-push-deb

packagecloud-autobuilds:
	$(MAKE) packagecloud-push REPO=go-graphite/autobuilds

packagecloud-stable:
	$(MAKE) packagecloud-push REPO=go-graphite/stable

sum-files: | sha256sum md5sum

md5sum:
	md5sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > md5sum

sha256sum:
	sha256sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > sha256sum
