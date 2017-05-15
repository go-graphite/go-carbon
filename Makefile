all: go-carbon

GO ?= go
export GOPATH := $(CURDIR)/_vendor

go-carbon:
	$(GO) build $(EXTRA_BUILD_FLAGS) github.com/lomik/go-carbon

tmp/go-carbon.tar.gz: go-carbon
	mkdir -p tmp/
	rm -rf tmp/go-carbon
	mkdir -p tmp/go-carbon/

	cp go-carbon tmp/go-carbon/go-carbon
	./go-carbon --config-print-default > tmp/go-carbon/go-carbon.conf
	cp deploy/go-carbon.init.centos tmp/go-carbon/go-carbon.init
	cp deploy/go-carbon.service tmp/go-carbon/go-carbon.service
	cp conf-examples/schemas tmp/go-carbon/storage-schemas.conf
	cp conf-examples/aggregation tmp/go-carbon/storage-aggregation.conf

	cd tmp && tar czf go-carbon.tar.gz go-carbon/

rpm: tmp/go-carbon.tar.gz
	cp deploy/buildrpm.sh tmp/buildrpm.sh
	cd tmp && ./buildrpm.sh ../deploy/go-carbon.spec `../go-carbon --version`

deb:
	DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage -B -us -uc

submodules:
	git submodule init
	git submodule update --recursive

test:
	find . -not -path "*vendor*" -name "*.go" | rev | cut -d/ -f2- | rev | cut -d/ -f2- | sort -u | xargs -IMODULE $(GO) test -race github.com/lomik/go-carbon/MODULE
	find . -not -path "*vendor*" -name "*.go" | rev | cut -d/ -f2- | rev | cut -d/ -f2- | sort -u | xargs -IMODULE $(GO) vet github.com/lomik/go-carbon/MODULE

clean:
	rm -f go-carbon

image:
	CGO_ENABLED=0 GOOS=linux $(MAKE) go-carbon
	docker build -t go-carbon .
