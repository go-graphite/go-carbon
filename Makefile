all: go-carbon

GO ?= go
export GOPATH := $(CURDIR)/_vendor

go-carbon:
	$(GO) build

tmp/go-carbon.tar.gz: go-carbon
	mkdir -p tmp/
	rm -rf tmp/go-carbon
	mkdir -p tmp/go-carbon/
	cp go-carbon tmp/go-carbon/go-carbon
	./go-carbon --config-print-default > tmp/go-carbon/go-carbon.conf
	cp deploy/go-carbon.init.centos tmp/go-carbon/go-carbon.init
	cp deploy/go-carbon.logrotate.centos tmp/go-carbon/go-carbon.logrotate
	cd tmp && tar czf go-carbon.tar.gz go-carbon/

rpm: tmp/go-carbon.tar.gz
	cp deploy/buildrpm.sh tmp/buildrpm.sh
	cd tmp && ./buildrpm.sh ../deploy/go-carbon.spec.centos `../go-carbon --version`

deb:
	DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage -B -us -uc

submodules:
	git submodule init
	git submodule update --recursive

test:
	$(GO) test ./...

clean:
	rm -f go-carbon

image:
	CGO_ENABLED=0 GOOS=linux $(MAKE) go-carbon
	docker build -t go-carbon .
