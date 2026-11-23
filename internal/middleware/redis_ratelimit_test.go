package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestRedisRateLimitSharedAndUnavailable(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL not set")
	}
	ctx := context.Background()
	prefix := fmt.Sprintf("test-limit-%d", time.Now().UnixNano())
	a, err := NewRedisRateLimiter(ctx, url, 2, 2, prefix)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewRedisRateLimiter(ctx, url, 2, 2, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = a.client.Del(ctx, prefix+":ratelimit:192.0.2.1").Err()
		_ = a.Close()
		_ = b.Close()
	})
	request := func(l *RedisRateLimiter) int {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		l.Limit(ok)(rec, req)
		return rec.Code
	}
	if request(a) != 200 || request(b) != 200 || request(a) != 429 {
		t.Fatal("limit must be shared across clients")
	}
	time.Sleep(600 * time.Millisecond)
	if got := request(b); got != 200 {
		t.Fatalf("refill: got %d", got)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if got := request(b); got != 503 {
		t.Fatalf("unavailable Redis must fail closed, got %d", got)
	}
}
