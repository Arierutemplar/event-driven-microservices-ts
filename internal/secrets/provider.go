package secrets

import (
	"fmt"
	"os"
	"strings"
)

// Provider resolves a named secret. Implementations must not disclose its value in errors.
type Provider interface {
	Get(name string) (string, error)
}

// Environment supports ESO-injected variables and files rendered by Vault Agent or CSI.
// Each mounted file is selected explicitly via NAME_FILE and read at startup.
type Environment struct{}

func (Environment) Get(name string) (string, error) {
	value, present := os.LookupEnv(name)
	path := os.Getenv(name + "_FILE")
	if path != "" && present && value != "" {
		return "", fmt.Errorf("%s and %s_FILE are mutually exclusive", name, name)
	}
	if path == "" {
		return value, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s_FILE: %w", name, err)
	}
	return strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r"), nil
}

// Load resolves the given names once, before the service starts its goroutines.
func Load(p Provider, names []string) error {
	values := make([]string, len(names))
	for i, name := range names {
		value, err := p.Get(name)
		if err != nil {
			return err
		}
		values[i] = value
	}
	for i, name := range names {
		if err := os.Setenv(name, values[i]); err != nil {
			return fmt.Errorf("set %s: %w", name, err)
		}
	}
	return nil
}
