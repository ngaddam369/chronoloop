CRATE := chronoloop

.PHONY: build fmt lint test bench audit verify clean

## build: compile the crate and all its targets
build:
	cargo build --all-targets

## fmt: format all Rust source files in place
fmt:
	cargo fmt --all

## lint: run clippy over every target, treating warnings as errors
lint:
	cargo clippy --all-targets -- -D warnings

## test: run all tests, including doctests
test:
	cargo test --all-targets
	cargo test --doc

## bench: run the benchmark suite (no benches until Phase 2)
bench:
	cargo bench

## audit: check dependencies for advisories, licenses, and banned crates
audit:
	@command -v cargo-deny >/dev/null 2>&1 || { \
	  echo "cargo-deny is not installed."; \
	  echo "  Arch:  sudo pacman -S cargo-deny     (prebuilt, instant)"; \
	  echo "  Other: cargo install cargo-deny      (compiles from source, slow)"; \
	  exit 1; \
	}
	cargo deny check

## verify: run the full checklist (fmt → build → lint → test → audit)
verify: fmt build lint test audit

## clean: remove build artifacts
clean:
	cargo clean
