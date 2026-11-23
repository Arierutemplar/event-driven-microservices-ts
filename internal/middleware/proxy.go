package middleware

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// TrustedProxies accepts forwarding headers only from the configured proxy networks.
func TrustedProxies(raw string, next http.Handler) (http.Handler, error) {
	var networks []netip.Prefix
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(part)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", part, err)
		}
		networks = append(networks, prefix.Masked())
	}
	trusted := func(ip netip.Addr) bool {
		for _, prefix := range networks {
			if prefix.Contains(ip.Unmap()) {
				return true
			}
		}
		return false
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		peer, err := netip.ParseAddr(host)
		if err == nil && trusted(peer) {
			// Walk right-to-left; a client cannot choose a hop beyond the first untrusted proxy.
			hops := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
			for i := len(hops) - 1; i >= 0; i-- {
				ip, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
				if err != nil {
					break
				}
				r.RemoteAddr = net.JoinHostPort(ip.String(), "0")
				if !trusted(ip) {
					break
				}
			}
			proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
			if proto == "http" || proto == "https" {
				r.URL.Scheme = proto
			}
		}
		next.ServeHTTP(w, r)
	}), nil
}
