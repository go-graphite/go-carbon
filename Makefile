NAME:=go-carbon
MAINTAINER:="Roman Lomonosov <r.lomonosov@gmail.com>"
DESCRIPTION:="Golang implementation of Graphite/Carbon server"
MODULE:=github.com/lomik/go-carbon

GO ?= go
export GOPATH := $(CURDIR)/_vendor
TEMPDIR:=$(shell mktemp -d)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')
BUILD ?= $(shell git describe --abbrev=4 --dirty --always --tags)

SOURCES=$(shell find . -name "*.go")

all: $(NAME)

$(NAME): $(SOURCES)
	$(GO) build --ldflags '-X main.BuildVersion=$(BUILD)' $(MODULE)

debug: $(SOURCES)
	$(GO) build -ldflags=-compressdwarf=false -gcflags=all='-l -N' $(MODULE)

run-test:
	$(GO) $(COMMAND) $(MODULE)
	$(GO) $(COMMAND) $(MODULE)/cache
	$(GO) $(COMMAND) $(MODULE)/carbon
	$(GO) $(COMMAND) $(MODULE)/carbonserver
	$(GO) $(COMMAND) $(MODULE)/helper/carbonzipperpb
	$(GO) $(COMMAND) $(MODULE)/helper
	$(GO) $(COMMAND) $(MODULE)/helper/qa
	$(GO) $(COMMAND) $(MODULE)/helper/stat
	$(GO) $(COMMAND) $(MODULE)/persister
	$(GO) $(COMMAND) $(MODULE)/points
	$(GO) $(COMMAND) $(MODULE)/tags
	$(GO) $(COMMAND) $(MODULE)/receiver
	$(GO) $(COMMAND) $(MODULE)/receiver/tcp
	$(GO) $(COMMAND) $(MODULE)/receiver/udp
	$(GO) $(COMMAND) $(MODULE)/receiver/parse
	$(GO) $(COMMAND) $(MODULE)/receiver/http

test:
	make run-test COMMAND="test"
	make run-test COMMAND="vet"
	# make run-test COMMAND="test -race"

gox-build:
	rm -rf build
	mkdir -p build
	cd build && $(GO) build $(MODULE)
	gox -os="linux" -arch="amd64" -arch="386" -output="build/$(NAME)-{{.OS}}-{{.Arch}}" $(MODULE)
	ls -la ./build/

clean:
	rm -f go-carbon

image:
	CGO_ENABLED=0 GOOS=linux $(MAKE) go-carbon
	docker build -t go-carbon .

package-tree:
	install -m 0755 -d build/root/lib/systemd/system
	install -m 0755 -d build/root/etc/$(NAME)
	install -m 0755 -d build/root/etc/logrotate.d
	install -m 0755 -d build/root/etc/init.d
	install -m 0644 deploy/$(NAME).service build/root/lib/systemd/system/$(NAME).service
	install -m 0644 deploy/$(NAME).conf build/root/etc/$(NAME)/$(NAME).conf
	install -m 0644 deploy/storage-schemas.conf build/root/etc/$(NAME)/storage-schemas.conf
	install -m 0644 deploy/storage-aggregation.conf build/root/etc/$(NAME)/storage-aggregation.conf
	install -m 0644 deploy/$(NAME).logrotate build/root/etc/logrotate.d/$(NAME)
	install -m 0755 deploy/$(NAME).init build/root/etc/init.d/$(NAME)

fpm-deb:
	make fpm-build-deb ARCH=amd64
	make fpm-build-deb ARCH=386

fpm-rpm:
	make fpm-build-rpm ARCH=amd64 FILE_ARCH=x86_64
	make fpm-build-rpm ARCH=386 FILE_ARCH=386

fpm-build-deb:
	make package-tree
	chmod 0755 build/$(NAME)-linux-$(ARCH)
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) \
		--deb-priority optional --category admin \
		--package $(NAME)_$(VERSION)_$(ARCH).deb \
		--force \
		--deb-compression bzip2 \
		--url https://github.com/lomik/$(NAME) \
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
		--url https://github.com/lomik/$(NAME) \
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
