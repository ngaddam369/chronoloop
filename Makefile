CRATE := chronoloop

.PHONY: build fmt lint test bench audit verify local-validation clean

## build: compile the crate and all its targets
build:
	cargo build --all-targets

## fmt: format all Rust source files in place
fmt:
	cargo fmt --all

## lint: run clippy over every target, treating warnings as errors
lint:
	cargo clippy --all-targets -- -D warnings

## test: run all tests, including doctests (never the benches — those are `make bench`)
test:
	cargo test --lib --bins --tests
	cargo test --doc

## bench: run the benchmark suite (it reports; it never asserts, and it never runs in CI)
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

## local-validation: the gated pass — every #[ignore]d test, in both profiles
## The gate is the attribute rather than the file: anything #[ignore]d is skipped by `make test` and
## by CI, and run here instead, so a check can live beside the thing it is about. The release pass is
## what turns "identical in debug and in release" into something checked rather than claimed.
## Targets are named, as everywhere else, so that a bench is never selected.
local-validation:
	cargo test --lib --bins --tests -- --include-ignored
	cargo test --lib --bins --tests --release -- --include-ignored

## clean: remove build artifacts
clean:
	cargo clean
