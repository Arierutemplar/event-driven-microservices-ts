package transport

import (
	"crypto/tls"
	"testing"
)

func TestClientTLSValidation(t *testing.T) {
	cfg, err := ClientTLS("", "", "")
	if err != nil || cfg.MinVersion != tls.VersionTLS12 || cfg.InsecureSkipVerify {
		t.Fatalf("unexpected TLS config: %#v %v", cfg, err)
	}
	if _, err := ClientTLS("", "missing", ""); err == nil {
		t.Fatal("missing client key accepted")
	}
	if _, err := ClientTLS("missing", "", ""); err == nil {
		t.Fatal("missing CA accepted")
	}
}
