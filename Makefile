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
	git submodule init
	git submodule update --recursive

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
	make fpm-build-deb ARCH=amd64 INIT=systemd
	make fpm-build-deb ARCH=386 INIT=systemd
	make fpm-build-deb ARCH=amd64 INIT=initd
	make fpm-build-deb ARCH=386 INIT=initd

fpm-rpm:
	make fpm-build-rpm ARCH=amd64 FILE_ARCH=x86_64 INIT=systemd
	make fpm-build-rpm ARCH=386 FILE_ARCH=386 INIT=systemd
	make fpm-build-rpm ARCH=amd64 FILE_ARCH=x86_64 INIT=initd
	make fpm-build-rpm ARCH=386 FILE_ARCH=386 INIT=initd

fpm-build-deb:
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) \
		--deb-priority optional --category admin \
		--package $(NAME)_$(VERSION)_$(ARCH)_$(INIT).deb \
		--force \
		--deb-compression bzip2 \
		--url https://github.com/lomik/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--before-install deploy/$(INIT)/scripts/before_install.sh \
		--before-upgrade deploy/$(INIT)/scripts/before_install.sh \
		--after-install deploy/$(INIT)/scripts/after_install.sh \
		--after-upgrade deploy/$(INIT)/scripts/after_install.sh \
		--config-files /etc/$(NAME)/$(NAME).conf \
		build/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/$(INIT)/root/=/ \
		deploy/etc/=/etc/$(NAME)/ \
		build/root/=/

fpm-build-rpm:
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) \
		--package $(NAME)-$(VERSION)-1.$(INIT).$(FILE_ARCH).rpm \
		--force \
		--rpm-compression bzip2 --rpm-os linux \
		--url https://github.com/lomik/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--before-install deploy/$(INIT)/scripts/before_install.sh \
		--before-upgrade deploy/$(INIT)/scripts/before_install.sh \
		--after-install deploy/$(INIT)/scripts/after_install.sh \
		--after-upgrade deploy/$(INIT)/scripts/after_install.sh \
		--config-files /etc/$(NAME)/$(NAME).conf \
		build/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/$(INIT)/root/=/ \
		deploy/etc/=/etc/$(NAME)/ \
		build/root/=/
