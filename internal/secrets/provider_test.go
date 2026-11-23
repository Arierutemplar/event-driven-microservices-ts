package secrets

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnvironmentFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("secret value\n"), 0600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("TEST_SECRET", "")
	t.Setenv("TEST_SECRET_FILE", path)
	got, err := (Environment{}).Get("TEST_SECRET")
	if err != nil || got != "secret value" {
		t.Fatalf("Get: %q, %v", got, err)
	}
	t.Setenv("TEST_SECRET", "another")
	if _, err := (Environment{}).Get("TEST_SECRET"); err == nil {
		t.Fatal("expected conflicting sources to fail")
	}
	t.Setenv("TEST_SECRET", "")
	t.Setenv("TEST_SECRET_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := (Environment{}).Get("TEST_SECRET"); err == nil {
		t.Fatal("expected missing file to fail")
	}
}

func TestLoadVaultAgentFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "api-key")
	if err := os.WriteFile(path, []byte("vault-value"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VAULT_TEST_KEY", "")
	t.Setenv("VAULT_TEST_KEY_FILE", path)
	t.Setenv("VAULT_TEST_OTHER", "")
	t.Setenv("VAULT_TEST_OTHER_FILE", filepath.Join(dir, "missing"))
	if err := Load(Environment{}, []string{"VAULT_TEST_KEY", "VAULT_TEST_OTHER"}); err == nil {
		t.Fatal("expected missing file to fail")
	}
	if got := os.Getenv("VAULT_TEST_KEY"); got != "" {
		t.Fatal("failed load applied an earlier secret")
	}
	t.Setenv("VAULT_TEST_OTHER_FILE", "")
	if err := Load(Environment{}, []string{"VAULT_TEST_KEY", "VAULT_TEST_OTHER"}); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("VAULT_TEST_KEY"); got != "vault-value" {
		t.Fatal("mounted secret was not loaded")
	}
}
