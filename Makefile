NAME:=go-carbon
MAINTAINER:="Roman Lomonosov <r.lomonosov@gmail.com>"
DESCRIPTION:="Golang implementation of Graphite/Carbon server"
MODULE:=github.com/go-graphite/go-carbon

GO ?= go
TEMPDIR:=$(shell mktemp -d)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')
BUILD ?= $(shell git describe --abbrev=4 --dirty --always --tags)

all: $(NAME)

$(NAME):
	$(GO) build -mod vendor --ldflags '-X main.BuildVersion=$(BUILD)' $(MODULE)

debug:
	$(GO) build -mod vendor -ldflags=-compressdwarf=false -gcflags=all='-l -N' $(MODULE)

run-test:
	$(GO) $(COMMAND) ./...

test:
	make run-test COMMAND="test"
	make run-test COMMAND="vet"
	make run-test COMMAND="test -race"

gox-build:
	rm -rf build
	mkdir -p build/root/etc/$(NAME)/
	cd build && $(GO) build $(MODULE)
	gox -os="linux" -arch="amd64" -arch="386" -arch="arm64" -output="build/$(NAME)-{{.OS}}-{{.Arch}}" $(MODULE)
	ls -la ./build/

clean:
	rm -f $(NAME) build/* *deb *rpm sha256sum md5sum

image:
	CGO_ENABLED=0 GOOS=linux $(MAKE) go-carbon
	docker build -t go-carbon .

package-tree:
	install -m 0755 -d build/root/lib/systemd/system
	install -m 0755 -d build/root/etc/$(NAME)
	install -m 0755 -d build/root/etc/logrotate.d
	install -m 0755 -d build/root/etc/init.d
	install -m 0644 deploy/$(NAME).service build/root/lib/systemd/system/$(NAME).service
	./build/$(NAME)-linux-amd64 -config-print-default > deploy/$(NAME).conf
	install -m 0644 deploy/$(NAME).conf build/root/etc/$(NAME)/$(NAME).conf
	install -m 0644 deploy/storage-schemas.conf build/root/etc/$(NAME)/storage-schemas.conf
	install -m 0644 deploy/storage-aggregation.conf build/root/etc/$(NAME)/storage-aggregation.conf
	install -m 0644 deploy/$(NAME).logrotate build/root/etc/logrotate.d/$(NAME)
	install -m 0755 deploy/$(NAME).init build/root/etc/init.d/$(NAME)

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
	chmod 0755 build/$(NAME)-linux-$(ARCH)
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
		build/root/=/ \
		build/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME)

fpm-build-rpm:
	make package-tree
	chmod 0755 build/$(NAME)-linux-$(ARCH)
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
		build/root/=/ \
		build/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME)

.ONESHELL:
RPM_VERSION:=$(subst -,_,$(VERSION))
packagecloud-push-rpm: $(wildcard $(NAME)-$(RPM_VERSION)-1.*.rpm)
	for pkg in $^; do
		package_cloud push $(REPO)/el/7 $${pkg} || true
		package_cloud push $(REPO)/el/8 $${pkg} || true
	done

.ONESHELL:
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
	make packagecloud-push-rpm
	make packagecloud-push-deb

packagecloud-autobuilds:
	make packagecloud-push REPO=go-graphite/autobuilds

packagecloud-stable:
	make packagecloud-push REPO=go-graphite/stable

sum-files: | sha256sum md5sum

md5sum:
	md5sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > md5sum

sha256sum:
	sha256sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > sha256sum