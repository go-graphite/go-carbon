NAME:=go-carbon
MAINTAINER:="Roman Lomonosov <r.lomonosov@gmail.com>"
DESCRIPTION:="Golang implementation of Graphite/Carbon server"
MODULE:=github.com/lomik/go-carbon

GO ?= go
export GOPATH := $(CURDIR)/_vendor
TEMPDIR:=$(shell mktemp -d)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')

all: $(NAME)

submodules:
	git submodule sync
	git submodule update --init --recursive

$(NAME):
	$(GO) build $(MODULE)

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
	$(GO) $(COMMAND) $(MODULE)/receiver

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

fpm-deb:
	make fpm-build-deb ARCH=amd64
	make fpm-build-deb ARCH=386

fpm-rpm:
	make fpm-build-rpm ARCH=amd64 FILE_ARCH=x86_64
	make fpm-build-rpm ARCH=386 FILE_ARCH=386

fpm-build-deb:
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
		--config-files /etc/$(NAME)/ \
		--config-files /etc/logrotate.d/$(NAME) \
		--deb-init deploy/$(NAME).init \
		build/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/$(NAME).service=/lib/systemd/system/$(NAME).service \
		deploy/$(NAME).conf=/etc/$(NAME)/$(NAME).conf \
		deploy/$(NAME).logrotate=/etc/logrotate.d/$(NAME) \
		deploy/storage-schemas.conf=/etc/$(NAME)/storage-schemas.conf \
		deploy/storage-aggregation.conf=/etc/$(NAME)/storage-aggregation.conf

fpm-build-rpm:
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
		--config-files /etc/$(NAME)/ \
		--config-files /etc/logrotate.d/$(NAME) \
		--rpm-init deploy/$(NAME).init \
		build/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/$(NAME).service=/lib/systemd/system/$(NAME).service \
		deploy/$(NAME).conf=/etc/$(NAME)/$(NAME).conf \
		deploy/$(NAME).logrotate=/etc/logrotate.d/$(NAME) \
		deploy/storage-schemas.conf=/etc/$(NAME)/storage-schemas.conf \
		deploy/storage-aggregation.conf=/etc/$(NAME)/storage-aggregation.conf
