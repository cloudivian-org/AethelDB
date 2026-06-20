# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Top-level developer entry point. Run `make help` for the list of targets.

CARGO ?= cargo

.PHONY: help build check test fmt fmt-check clippy compute-image images up down e2e clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Compile the Rust workspace (proxy, safekeeper, pageserver)
	$(CARGO) build --workspace

check: ## Type-check the Rust workspace without producing binaries
	$(CARGO) check --workspace

test: ## Run all Rust unit/integration tests
	$(CARGO) test --workspace

fmt: ## Format all Rust code
	$(CARGO) fmt --all

fmt-check: ## Verify formatting (CI gate)
	$(CARGO) fmt --all -- --check

clippy: ## Lint with clippy, denying warnings
	$(CARGO) clippy --workspace --all-targets -- -D warnings

compute-image: ## Build the patchable PostgreSQL compute container image
	docker build -t aetheldb/compute:dev ./compute

images: ## Build the proxy/safekeeper/pageserver service images (for k8s)
	docker build -t aetheldb/proxy:dev      --build-arg BIN=aethel-proxy      -f deploy/Dockerfile.rust .
	docker build -t aetheldb/safekeeper:dev --build-arg BIN=aethel-safekeeper -f deploy/Dockerfile.rust .
	docker build -t aetheldb/pageserver:dev --build-arg BIN=aethel-pageserver -f deploy/Dockerfile.rust .

up: ## Start the local stack (safekeeper, pageserver, object store) via compose
	docker compose up --build -d

down: ## Stop the local stack
	docker compose down -v

e2e: ## Run the Python end-to-end lifecycle tests (Step 6)
	cd e2e-tests && python -m pytest -v

clean: ## Remove Rust build artifacts and local node data
	$(CARGO) clean
	rm -rf .data
