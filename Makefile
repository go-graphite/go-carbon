NAME:=go-carbon
MAINTAINER:="Roman Lomonosov <r.lomonosov@gmail.com>"
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

gox-build:
	rm -rf out
	mkdir -p out
	cd out && $(GO) build $(MODULE) && cd ..
	gox -os="linux" -arch="amd64" -arch="386" -arch="arm64" -output="out/$(NAME)-{{.OS}}-{{.Arch}}" $(MODULE)
	ls -la ./out/

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

fpm-deb:
	make fpm-build-deb ARCH=amd64
	make fpm-build-deb ARCH=386
	make fpm-build-deb ARCH=arm64

fpm-rpm:
	make fpm-build-rpm ARCH=amd64 FILE_ARCH=x86_64
	make fpm-build-rpm ARCH=386 FILE_ARCH=386
	make fpm-build-rpm ARCH=arm64 FILE_ARCH=arm64

fpm-build-deb:
	make package-tree
	chmod 0755 out/$(NAME)-linux-$(ARCH)
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) \
		--deb-priority optional --category admin \
		--package $(NAME)_$(VERSION)_$(ARCH).deb \
		--force \
		--deb-compression bzip2 \
		--url https://github.com/go-graphite/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--before-install deploy/before_install.sh \
		--before-upgrade deploy/before_install.sh \
		--after-install deploy/after_install.sh \
		--after-upgrade deploy/after_install.sh \
		--config-files /etc/ \
		out/root/=/ \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME)

fpm-build-rpm:
	make package-tree
	chmod 0755 out/$(NAME)-linux-$(ARCH)
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) \
		--package $(NAME)-$(VERSION)-1.$(FILE_ARCH).rpm \
		--force \
		--rpm-compression bzip2 --rpm-os linux \
		--url https://github.com/go-graphite/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--before-install deploy/before_install.sh \
		--before-upgrade deploy/before_install.sh \
		--after-install deploy/after_install.sh \
		--after-upgrade deploy/after_install.sh \
		--config-files /etc/ \
		out/root/=/ \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME)

packagecloud-push-rpm: $(wildcard $(NAME)-$(RPM_VERSION)-1.*.rpm)
	for pkg in $^; do
		package_cloud push $(REPO)/el/7 $${pkg} || true
		package_cloud push $(REPO)/el/8 $${pkg} || true
	done

packagecloud-push-deb: $(wildcard $(NAME)_$(VERSION)_*.deb)
	for pkg in $^; do
		package_cloud push $(REPO)/ubuntu/xenial   $${pkg} || true
		package_cloud push $(REPO)/ubuntu/bionic   $${pkg} || true
		package_cloud push $(REPO)/ubuntu/focal    $${pkg} || true
		package_cloud push $(REPO)/debian/stretch  $${pkg} || true
		package_cloud push $(REPO)/debian/buster   $${pkg} || true
		package_cloud push $(REPO)/debian/bullseye $${pkg} || true
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
