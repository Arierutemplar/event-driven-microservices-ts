package middleware

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

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
	once   sync.Once
}

func NewRedisRateLimiter(ctx context.Context, url string, rps float64, burst int, prefix string) (*RedisRateLimiter, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}
	if prefix == "" {
		prefix = "analytics"
	}
	return &RedisRateLimiter{client: client, rps: rps, burst: burst, prefix: prefix}, nil
}

func (l *RedisRateLimiter) Close() error { return l.client.Close() }

func (l *RedisRateLimiter) allow(ctx context.Context, ip string) bool {
	_ = l.rps
	key := l.prefix + ":ratelimit:" + ip
	window := l.burst
	count, err := l.client.Incr(ctx, key).Result()
	if err != nil {
		return true
	}
	if count == 1 {
		l.client.Expire(ctx, key, time.Second)
	}
	return count <= int64(window)
}

func (l *RedisRateLimiter) Limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		if !l.allow(r.Context(), ip) {
			w.Header().Set("Retry-After", strconv.Itoa(1))
			http.Error(w, "Demasiadas peticiones", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}
