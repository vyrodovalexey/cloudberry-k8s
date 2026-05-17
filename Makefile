# =============================================================================
# Cloudberry Kubernetes Operator - Makefile
# =============================================================================

# --- Project settings --------------------------------------------------------
PROJECT_NAME     := cloudberry-k8s
MODULE           := github.com/cloudberry-contrib/cloudberry-k8s
BIN_DIR          := bin

# --- Version info ------------------------------------------------------------
VERSION          ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT           := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE       := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# --- Go settings -------------------------------------------------------------
GO               := go
GOFLAGS          ?=
CGO_ENABLED      ?= 0
GOOS             ?= $(shell $(GO) env GOOS)
GOARCH           ?= $(shell $(GO) env GOARCH)
LDFLAGS          := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)

# --- Docker settings ---------------------------------------------------------
IMG_OPERATOR     ?= cloudberry-operator:latest
IMG_CTL          ?= cloudberry-ctl:latest
IMG_CLOUDBERRY   ?= cloudberrydb/cloudberry:2.1.0
DOCKER           ?= docker
DOCKER_BUILD_ARGS := --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE)

# --- Kubernetes / Helm settings ----------------------------------------------
NAMESPACE_OPERATOR ?= cloudberry-system
NAMESPACE_TEST     ?= cloudberry-test
HELM_RELEASE       ?= cloudberry-operator
HELM_CHART         := deploy/helm/cloudberry-operator

# --- Tool versions -----------------------------------------------------------
GOLANGCI_LINT_VERSION ?= v1.64.8
CONTROLLER_GEN_VERSION ?= v0.17.3

# --- Tool binaries -----------------------------------------------------------
GOLANGCI_LINT    := $(shell command -v golangci-lint 2>/dev/null)
CONTROLLER_GEN   := $(shell command -v controller-gen 2>/dev/null)
HELM             := $(shell command -v helm 2>/dev/null)
GOVULNCHECK      := $(shell command -v govulncheck 2>/dev/null)

# =============================================================================
# Default target
# =============================================================================
.DEFAULT_GOAL := help

# =============================================================================
# Build targets
# =============================================================================

.PHONY: build
build: build-operator build-ctl ## Build both operator and cloudberry-ctl binaries

.PHONY: build-operator
build-operator: ## Build operator binary
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build \
		-trimpath \
		-ldflags="$(LDFLAGS)" \
		-o $(BIN_DIR)/cloudberry-operator \
		./cmd/operator/

.PHONY: build-ctl
build-ctl: ## Build cloudberry-ctl binary
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build \
		-trimpath \
		-ldflags="$(LDFLAGS)" \
		-o $(BIN_DIR)/cloudberry-ctl \
		./cmd/cloudberry-ctl/

# =============================================================================
# Test targets
# =============================================================================

.PHONY: test
test: ## Run unit tests
	$(GO) test ./api/... ./cmd/... ./internal/... -race -count=1 -v

.PHONY: test-cover
test-cover: ## Run unit tests with coverage report
	@mkdir -p coverage
	$(GO) test ./api/... ./cmd/... ./internal/... \
		-race -count=1 -v \
		-coverprofile=coverage/coverage.out \
		-covermode=atomic
	$(GO) tool cover -html=coverage/coverage.out -o coverage/coverage.html
	$(GO) tool cover -func=coverage/coverage.out

.PHONY: test-functional
test-functional: ## Run functional tests
	$(GO) test ./test/functional/... -tags=functional -race -count=1 -v -timeout=10m

.PHONY: test-integration
test-integration: ## Run integration tests (requires docker-compose)
	$(GO) test ./test/integration/... -tags=integration -race -count=1 -v -timeout=15m

.PHONY: test-e2e
test-e2e: ## Run e2e tests
	$(GO) test ./test/e2e/... -tags=e2e -race -count=1 -v -timeout=30m

.PHONY: test-all
test-all: test test-functional test-integration test-e2e ## Run all tests

# =============================================================================
# Lint & Quality targets
# =============================================================================

.PHONY: lint
lint: ## Run golangci-lint
ifndef GOLANGCI_LINT
	@echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
endif
	golangci-lint run ./...

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Run gofmt
	gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Check gofmt formatting
	@test -z "$$(gofmt -l .)" || (echo "Files not formatted:"; gofmt -l .; exit 1)

.PHONY: vuln
vuln: ## Run govulncheck
ifndef GOVULNCHECK
	@echo "Installing govulncheck..."
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest
endif
	govulncheck ./...

# =============================================================================
# Docker targets
# =============================================================================

.PHONY: docker-build
docker-build: docker-build-operator docker-build-ctl ## Build Docker images for operator and ctl

.PHONY: docker-build-all
docker-build-all: docker-build docker-build-cloudberry ## Build all Docker images (operator, ctl, cloudberry)

.PHONY: docker-push
docker-push: ## Push Docker images
	$(DOCKER) push $(IMG_OPERATOR)
	$(DOCKER) push $(IMG_CTL)

.PHONY: docker-push-cloudberry
docker-push-cloudberry: ## Push Cloudberry database image
	$(DOCKER) push $(IMG_CLOUDBERRY)

.PHONY: docker-build-operator
docker-build-operator: ## Build operator Docker image
	$(DOCKER) build \
		$(DOCKER_BUILD_ARGS) \
		-t $(IMG_OPERATOR) \
		-f Dockerfile.operator .

.PHONY: docker-build-ctl
docker-build-ctl: ## Build cloudberry-ctl Docker image
	$(DOCKER) build \
		$(DOCKER_BUILD_ARGS) \
		-t $(IMG_CTL) \
		-f Dockerfile.ctl .

.PHONY: docker-build-cloudberry
docker-build-cloudberry: ## Build Apache Cloudberry database image (compiles from source)
	$(DOCKER) build \
		-t $(IMG_CLOUDBERRY) \
		-f Dockerfile.cloudberry .

# =============================================================================
# Kubernetes / Helm targets
# =============================================================================

.PHONY: helm-lint
helm-lint: ## Lint Helm chart
	$(HELM) lint $(HELM_CHART)

.PHONY: helm-template
helm-template: ## Template Helm chart
	$(HELM) template $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(NAMESPACE_OPERATOR) \
		--set installCRDs=true

.PHONY: helm-install
helm-install: ## Install operator via Helm in cloudberry-system namespace
	kubectl create namespace $(NAMESPACE_OPERATOR) --dry-run=client -o yaml | kubectl apply -f -
	$(HELM) install $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(NAMESPACE_OPERATOR) \
		--set installCRDs=true \
		--wait --timeout 5m

.PHONY: helm-upgrade
helm-upgrade: ## Upgrade operator via Helm
	$(HELM) upgrade $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(NAMESPACE_OPERATOR) \
		--set installCRDs=true \
		--wait --timeout 5m

.PHONY: helm-uninstall
helm-uninstall: ## Uninstall operator
	$(HELM) uninstall $(HELM_RELEASE) --namespace $(NAMESPACE_OPERATOR) || true

.PHONY: deploy-cluster
deploy-cluster: ## Deploy sample CloudberryCluster in cloudberry-test namespace
	kubectl create namespace $(NAMESPACE_TEST) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deploy/helm/cloudberry-operator/config/samples/cloudberrycluster-sample.yaml \
		-n $(NAMESPACE_TEST)

.PHONY: undeploy-cluster
undeploy-cluster: ## Remove sample cluster
	kubectl delete -f deploy/helm/cloudberry-operator/config/samples/cloudberrycluster-sample.yaml \
		-n $(NAMESPACE_TEST) --ignore-not-found

# =============================================================================
# Code Generation targets
# =============================================================================

.PHONY: generate
generate: controller-gen ## Generate deepcopy, CRD manifests
	$(CONTROLLER_GEN) object paths="./api/..."
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=deploy/helm/cloudberry-operator/crds

.PHONY: manifests
manifests: controller-gen ## Generate CRD and RBAC manifests
	$(CONTROLLER_GEN) crd rbac:roleName=cloudberry-operator paths="./..." \
		output:crd:artifacts:config=deploy/helm/cloudberry-operator/crds

.PHONY: controller-gen
controller-gen: ## Install controller-gen if not present
ifndef CONTROLLER_GEN
	@echo "Installing controller-gen $(CONTROLLER_GEN_VERSION)..."
	$(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
endif

# =============================================================================
# Test Environment targets
# =============================================================================

.PHONY: test-env-up
test-env-up: ## Start test environment (docker-compose)
	$(DOCKER) compose -f test/docker-compose/docker-compose.yml up -d

.PHONY: test-env-down
test-env-down: ## Stop test environment
	$(DOCKER) compose -f test/docker-compose/docker-compose.yml down -v

.PHONY: test-env-setup
test-env-setup: ## Run Vault and Keycloak setup scripts
	@echo "Waiting for services to be ready..."
	@sleep 10
	bash test/docker-compose/scripts/setup-vault.sh
	bash test/docker-compose/scripts/setup-keycloak.sh

# =============================================================================
# Clean targets
# =============================================================================

.PHONY: clean
clean: ## Clean build artifacts
	rm -rf $(BIN_DIR)/cloudberry-operator $(BIN_DIR)/cloudberry-ctl
	rm -rf coverage/

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help: ## Show this help message
	@printf "\nUsage: make \033[36m<target>\033[0m\n\n"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf "\n"
