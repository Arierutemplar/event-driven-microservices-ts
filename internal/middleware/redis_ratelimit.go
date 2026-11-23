package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/transport"
	"github.com/redis/go-redis/v9"
)

type RequestLimiter interface {
	Limit(http.HandlerFunc) http.HandlerFunc
}

type RedisRateLimiter struct {
	client *redis.Client
	rps    float64
	burst  int
	prefix string
}

func NewRedisRateLimiter(ctx context.Context, url string, rps float64, burst int, prefix string) (*RedisRateLimiter, error) {
	if rps <= 0 || burst <= 0 {
		return nil, fmt.Errorf("rate and burst must be positive")
	}
	opts, err := transport.RedisOptions(url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	if prefix == "" {
		prefix = "analytics"
	}
	return &RedisRateLimiter{client: client, rps: rps, burst: burst, prefix: prefix}, nil
}

func (l *RedisRateLimiter) Close() error { return l.client.Close() }

var tokenBucket = redis.NewScript(`
local now = redis.call('TIME')
local ms = now[1] * 1000 + math.floor(now[2] / 1000)
local rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
local old = redis.call('HMGET', KEYS[1], 'tokens', 'time')
local tokens = tonumber(old[1]) or burst
local last = tonumber(old[2]) or ms
tokens = math.min(burst, tokens + math.max(0, ms - last) * rate / 1000)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'time', ms)
redis.call('PEXPIRE', KEYS[1], math.max(1000, math.ceil(burst * 1000 / rate)))
return allowed
`)

func (l *RedisRateLimiter) allow(ctx context.Context, ip string) (bool, error) {
	count, err := tokenBucket.Run(ctx, l.client, []string{l.prefix + ":ratelimit:" + ip}, l.rps, l.burst).Int()
	if err != nil {
		return false, fmt.Errorf("redis rate limiter: %w", err)
	}
	return count == 1, nil
}

func (l *RedisRateLimiter) Limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		allowed, err := l.allow(r.Context(), ip)
		if err != nil {
			slog.ErrorContext(r.Context(), "rate limiter no disponible", "error", err)
			http.Error(w, "Limitador no disponible", http.StatusServiceUnavailable)
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(1))
			http.Error(w, "Demasiadas peticiones", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}
