// Package testutil provides shared test utilities for functional, integration, and e2e tests.
package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	// EnvVaultAddr is the environment variable for the Vault address.
	EnvVaultAddr = "VAULT_ADDR"
	// EnvVaultToken is the environment variable for the Vault token.
	EnvVaultToken = "VAULT_TOKEN"
	// EnvKeycloakAddr is the environment variable for the Keycloak address.
	EnvKeycloakAddr = "KEYCLOAK_ADDR"
	// EnvKeycloakAdmin is the environment variable for the Keycloak admin username.
	EnvKeycloakAdmin = "KEYCLOAK_ADMIN"
	// EnvKeycloakAdminPassword is the environment variable for the Keycloak admin password.
	EnvKeycloakAdminPassword = "KEYCLOAK_ADMIN_PASSWORD"
	// EnvTempoEndpoint is the environment variable for the Tempo OTLP endpoint.
	EnvTempoEndpoint = "TEMPO_ENDPOINT"
	// EnvVictoriaMetricsAddr is the environment variable for the VictoriaMetrics address.
	EnvVictoriaMetricsAddr = "VICTORIAMETRICS_ADDR"
	// EnvDockerComposeDir is the environment variable for the docker-compose directory.
	EnvDockerComposeDir = "DOCKER_COMPOSE_DIR"
	// EnvSkipDockerCompose is the environment variable to skip docker-compose management.
	EnvSkipDockerCompose = "SKIP_DOCKER_COMPOSE"
	// EnvTestNamespace is the environment variable for the test Kubernetes namespace.
	EnvTestNamespace = "TEST_NAMESPACE"

	// DefaultVaultAddr is the default Vault address for testing.
	DefaultVaultAddr = "http://127.0.0.1:8200"
	// DefaultVaultToken is the default Vault token for testing.
	DefaultVaultToken = "myroot"
	// DefaultKeycloakAddr is the default Keycloak address for testing.
	DefaultKeycloakAddr = "http://127.0.0.1:8090"
	// DefaultKeycloakAdmin is the default Keycloak admin username.
	DefaultKeycloakAdmin = "admin"
	// DefaultKeycloakAdminPassword is the default Keycloak admin password.
	DefaultKeycloakAdminPassword = "admin"
	// DefaultTempoEndpoint is the default Tempo OTLP gRPC endpoint.
	DefaultTempoEndpoint = "127.0.0.1:4317"
	// DefaultVictoriaMetricsAddr is the default VictoriaMetrics address.
	DefaultVictoriaMetricsAddr = "http://127.0.0.1:8428"
	// DefaultTestNamespace is the default test namespace.
	DefaultTestNamespace = "cloudberry-test"
)

// TestEnv holds the test environment configuration.
type TestEnv struct {
	VaultAddr             string
	VaultToken            string
	KeycloakAddr          string
	KeycloakAdmin         string
	KeycloakAdminPassword string
	TempoEndpoint         string
	VictoriaMetricsAddr   string
	DockerComposeDir      string
	TestNamespace         string
	Logger                *slog.Logger
}

// NewTestEnv creates a new TestEnv from environment variables with defaults.
func NewTestEnv() *TestEnv {
	return &TestEnv{
		VaultAddr:             getEnvOrDefault(EnvVaultAddr, DefaultVaultAddr),
		VaultToken:            getEnvOrDefault(EnvVaultToken, DefaultVaultToken),
		KeycloakAddr:          getEnvOrDefault(EnvKeycloakAddr, DefaultKeycloakAddr),
		KeycloakAdmin:         getEnvOrDefault(EnvKeycloakAdmin, DefaultKeycloakAdmin),
		KeycloakAdminPassword: getEnvOrDefault(EnvKeycloakAdminPassword, DefaultKeycloakAdminPassword),
		TempoEndpoint:         getEnvOrDefault(EnvTempoEndpoint, DefaultTempoEndpoint),
		VictoriaMetricsAddr:   getEnvOrDefault(EnvVictoriaMetricsAddr, DefaultVictoriaMetricsAddr),
		DockerComposeDir:      getEnvOrDefault(EnvDockerComposeDir, findDockerComposeDir()),
		TestNamespace:         getEnvOrDefault(EnvTestNamespace, DefaultTestNamespace),
		Logger:                slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

// StartDockerCompose starts the docker-compose environment.
func (e *TestEnv) StartDockerCompose(ctx context.Context) error {
	if os.Getenv(EnvSkipDockerCompose) == "true" {
		e.Logger.Info("skipping docker-compose start (SKIP_DOCKER_COMPOSE=true)")
		return nil
	}

	e.Logger.Info("starting docker-compose environment", "dir", e.DockerComposeDir)

	cmd := exec.CommandContext(ctx, "docker", "compose", "up", "-d", "--wait")
	cmd.Dir = e.DockerComposeDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("starting docker-compose: %w", err)
	}

	e.Logger.Info("docker-compose environment started")
	return nil
}

// StopDockerCompose stops the docker-compose environment.
func (e *TestEnv) StopDockerCompose(ctx context.Context) error {
	if os.Getenv(EnvSkipDockerCompose) == "true" {
		e.Logger.Info("skipping docker-compose stop (SKIP_DOCKER_COMPOSE=true)")
		return nil
	}

	e.Logger.Info("stopping docker-compose environment", "dir", e.DockerComposeDir)

	cmd := exec.CommandContext(ctx, "docker", "compose", "down", "-v")
	cmd.Dir = e.DockerComposeDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("stopping docker-compose: %w", err)
	}

	e.Logger.Info("docker-compose environment stopped")
	return nil
}

// WaitForService waits for a service to become available at the given URL.
func (e *TestEnv) WaitForService(ctx context.Context, name, healthURL string, timeout time.Duration) error {
	e.Logger.Info("waiting for service", "name", name, "url", healthURL, "timeout", timeout)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled while waiting for %s", name)
		default:
		}

		cmd := exec.CommandContext(ctx, "curl", "-sf", "--max-time", "3", healthURL)
		if err := cmd.Run(); err == nil {
			e.Logger.Info("service is ready", "name", name)
			return nil
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("service %s not ready after %v", name, timeout)
}

// WaitForVault waits for Vault to become available.
func (e *TestEnv) WaitForVault(ctx context.Context) error {
	return e.WaitForService(ctx, "vault", e.VaultAddr+"/v1/sys/health", 60*time.Second)
}

// WaitForKeycloak waits for Keycloak to become available.
func (e *TestEnv) WaitForKeycloak(ctx context.Context) error {
	return e.WaitForService(ctx, "keycloak", e.KeycloakAddr+"/realms/master", 120*time.Second)
}

// RunSetupScript runs a setup script from the docker-compose scripts directory.
func (e *TestEnv) RunSetupScript(ctx context.Context, scriptName string) error {
	scriptPath := filepath.Join(e.DockerComposeDir, "scripts", scriptName)
	e.Logger.Info("running setup script", "script", scriptPath)

	cmd := exec.CommandContext(ctx, "bash", scriptPath)
	cmd.Dir = e.DockerComposeDir
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("VAULT_ADDR=%s", e.VaultAddr),
		fmt.Sprintf("VAULT_TOKEN=%s", e.VaultToken),
		fmt.Sprintf("KEYCLOAK_ADDR=%s", e.KeycloakAddr),
		fmt.Sprintf("KEYCLOAK_ADMIN=%s", e.KeycloakAdmin),
		fmt.Sprintf("KEYCLOAK_ADMIN_PASSWORD=%s", e.KeycloakAdminPassword),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("running setup script %s: %w", scriptName, err)
	}

	e.Logger.Info("setup script completed", "script", scriptName)
	return nil
}

// ContextWithTimeout creates a context with the given timeout.
func ContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// getEnvOrDefault returns the value of an environment variable or a default value.
func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// findDockerComposeDir finds the docker-compose directory relative to the project root.
func findDockerComposeDir() string {
	// Try to find from the current file location.
	_, filename, _, ok := runtime.Caller(0)
	if ok {
		testDir := filepath.Dir(filepath.Dir(filename))
		composeDir := filepath.Join(testDir, "docker-compose")
		if _, err := os.Stat(composeDir); err == nil {
			return composeDir
		}
	}

	// Fallback: try common paths.
	candidates := []string{
		"test/docker-compose",
		"../test/docker-compose",
		"../../test/docker-compose",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			abs, _ := filepath.Abs(c)
			return abs
		}
	}

	return "test/docker-compose"
}

// UniqueNamespace generates a unique namespace name for test isolation.
func UniqueNamespace(prefix string) string {
	return fmt.Sprintf("%s-%d", strings.ToLower(prefix), time.Now().UnixNano()%100000)
}
