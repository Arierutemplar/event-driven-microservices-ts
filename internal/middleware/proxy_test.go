package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTrustedProxies(t *testing.T) {
	var addr, scheme string
	h, err := TrustedProxies("10.0.0.0/8", http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		addr, scheme = r.RemoteAddr, r.URL.Scheme
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		peer, forwarded, proto, wantAddr, wantScheme string
	}{
		{"10.0.0.1:42", "203.0.113.1, 198.51.100.2, 10.1.1.1", "https", "198.51.100.2:0", "https"},
		{"192.0.2.1:42", "203.0.113.1", "https", "192.0.2.1:42", "http"},
		{"10.0.0.1:42", "203.0.113.1, invalid", "https", "10.0.0.1:42", "https"},
	} {
		req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
		req.RemoteAddr = tc.peer
		req.Header.Set("X-Forwarded-For", tc.forwarded)
		req.Header.Set("X-Forwarded-Proto", tc.proto)
		h.ServeHTTP(httptest.NewRecorder(), req)
		if addr != tc.wantAddr || scheme != tc.wantScheme {
			t.Errorf("peer %s: got %s %s, want %s %s", tc.peer, addr, scheme, tc.wantAddr, tc.wantScheme)
		}
	}
	if _, err := TrustedProxies("invalid", h); err == nil {
		t.Fatal("invalid CIDR accepted")
	}
}
