NAME:=go-carbon
DESCRIPTION:="Golang implementation of Graphite/Carbon server"
MODULE:=github.com/go-graphite/go-carbon

SHELL := bash
.ONESHELL:

GO ?= go
TEST_TIMEOUT ?= 30m
export GOFLAGS +=  -mod=vendor
export GO111MODULE := on

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

# Targets that name no file; "test" would otherwise be up to date because of
# the test/ directory, and run nothing.
.PHONY: all build-linux debug run-test test clean image package-tree gox-build \
	nfpm-deb nfpm-rpm packagecloud-push packagecloud-push-rpm packagecloud-push-deb \
	packagecloud-autobuilds packagecloud-stable sum-files vendor

# Retain dependency oracles; go mod vendor omits test files.
WHISPER_VENDOR_TESTS := \
	align_points_test.go \
	compress_bits_test.go \
	continuation_off_grid_test.go \
	existing_open_test.go \
	ooo_direct_test.go \
	overlap_differential_test.go \
	overlap_digest_test.go \
	overlap_kinds_test.go \
	overlap_late_tail_test.go \
	overlap_mix_test.go \
	overlap_shapes_test.go \
	rollup_scratch_test.go

# Regenerate dependencies and reapply the documented local dependency fixes.
vendor:
	$(GO) mod vendor
	whisper_version=$$($(GO) list -m -f '{{.Version}}' github.com/go-graphite/go-whisper); \
	whisper_source="$$($(GO) env GOMODCACHE)/github.com/go-graphite/go-whisper@$$whisper_version"; \
	for whisper_test in $(WHISPER_VENDOR_TESTS); do \
		cp "$$whisper_source/$$whisper_test" vendor/github.com/go-graphite/go-whisper/ || exit $$?; \
	done
	patch --batch --fuzz=0 -p1 < patches/whisper-oracle-fixtures.patch
	patch --batch --fuzz=0 -p1 < patches/vellum-builder-reuse.patch
	patch --batch --fuzz=0 -p1 < patches/pebble-async-wal-rotation.patch
	patch --batch --fuzz=0 -p1 < patches/pebble-explicit-cache-shards.patch
	patch --batch --fuzz=0 -p1 < patches/pebble-range-delete-cache.patch
	patch --batch --fuzz=0 -p1 < patches/pebble-provider-dir-sync.patch
	patch --batch --fuzz=0 -p1 < patches/protobuf-json-stable-spacing.patch
	patch --batch --fuzz=0 -p1 < patches/pebble-integration-delta.patch

run-test:
	./test/run-tests.sh $(GO) $(COMMAND)

test:
	make run-test COMMAND="test -timeout $(TEST_TIMEOUT)"
	make run-test COMMAND="vet"
	make run-test COMMAND="test -race -timeout $(TEST_TIMEOUT)"

.PHONY: test-storage bench-storage test-storage-clean bench-storage-clean
STORAGE_TMP_ROOT = $(if $(TMPDIR),$(TMPDIR),/tmp)/go-carbon-storage

# Keep each wrapper in one shell even on macOS make without .ONESHELL support.
test-storage:
	mkdir -p "$(STORAGE_TMP_ROOT)/test-storage" || exit $$?; \
	storage_run_tmp=$$(mktemp -d "$(STORAGE_TMP_ROOT)/test-storage/run.XXXXXX") || exit $$?; \
	trap 'rm -rf -- "$$storage_run_tmp"' EXIT; \
	TMPDIR="$$storage_run_tmp" $(GO) test -timeout $(TEST_TIMEOUT) -count=1 -race -run '^TestStorage' ./persister

# Override for fixed-work comparisons, e.g. STORAGE_BENCH_FLAGS='-benchtime=4096x -count=5'.
STORAGE_BENCH_FLAGS ?= -benchtime=1s -count=3
bench-storage:
	mkdir -p "$(STORAGE_TMP_ROOT)/bench-storage" || exit $$?; \
	storage_run_tmp=$$(mktemp -d "$(STORAGE_TMP_ROOT)/bench-storage/run.XXXXXX") || exit $$?; \
	trap 'rm -rf -- "$$storage_run_tmp"' EXIT; \
	TMPDIR="$$storage_run_tmp" $(GO) test -run '^$$' -bench '^BenchmarkStorage' -benchmem $(STORAGE_BENCH_FLAGS) ./persister

test-storage-clean:
	rm -rf -- "$(STORAGE_TMP_ROOT)/test-storage"

bench-storage-clean:
	rm -rf -- "$(STORAGE_TMP_ROOT)/bench-storage"

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
