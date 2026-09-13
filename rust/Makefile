.PHONY: build reference test check

build:
	cargo build --locked --release --workspace

reference:
	cd .. && go build -mod=vendor -o rust/target/go-whisper-reference ./rust/reference

test: reference
	cargo test --locked --workspace

check: test
	cargo fmt --all --check
	cargo clippy --locked --workspace --all-targets -- -D warnings
