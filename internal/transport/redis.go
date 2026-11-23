package transport

import (
	"crypto/tls"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

func RedisOptions(rawURL string) (*redis.Options, error) {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL")
	}
	ca, cert, key := os.Getenv("REDIS_TLS_CA_FILE"), os.Getenv("REDIS_TLS_CERT_FILE"), os.Getenv("REDIS_TLS_KEY_FILE")
	if ca != "" || cert != "" || key != "" {
		if opts.TLSConfig == nil {
			return nil, fmt.Errorf("redis TLS files require a rediss URL")
		}
		opts.TLSConfig, err = ClientTLS(ca, cert, key)
		if err != nil {
			return nil, fmt.Errorf("redis TLS: %w", err)
		}
	} else if opts.TLSConfig != nil {
		opts.TLSConfig.MinVersion = tls.VersionTLS12
	}
	return opts, nil
}
