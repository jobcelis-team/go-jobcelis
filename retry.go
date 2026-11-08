package jobcelis

import (
	"context"
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultMaxRetries is the number of times a failed request is retried
	// when the client is created with NewClient. With the default backoff
	// (1s, 2s, 4s, 8s, 16s, 30s) it covers about 60 seconds of downtime.
	DefaultMaxRetries = 6

	defaultRetryBaseDelay = time.Second
	maxRetryDelay         = 30 * time.Second
	maxRetryAfter         = 60 * time.Second

	idempotencyKeyHeader = "X-Idempotency-Key"
)

// WithMaxRetries sets how many times a failed request is retried
// (network errors and HTTP 429, 502, 503 and 504). 0 disables retries.
func (c *Client) WithMaxRetries(maxRetries int) *Client {
	if maxRetries < 0 {
		maxRetries = 0
	}
	c.MaxRetries = maxRetries
	return c
}

// isRetryableStatus reports whether an HTTP status means the server did not
// process the request and it can be sent again.
func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// isIdempotent reports whether a request can be retried after a network
// error, when it is unknown whether the server processed it.
func isIdempotent(method string, header http.Header) bool {
	if method != http.MethodPost {
		return true
	}
	return header.Get(idempotencyKeyHeader) != ""
}

// retryDelay returns how long to wait before retry number attempt (1-based).
// A Retry-After header, when present, wins over the exponential backoff.
func (c *Client) retryDelay(attempt int, resp *http.Response) time.Duration {
	if resp != nil {
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return d
		}
	}

	base := c.retryBaseDelay
	if base <= 0 {
		base = defaultRetryBaseDelay
	}
	limit := (maxRetryDelay / defaultRetryBaseDelay) * base

	delay := base
	for i := 1; i < attempt && delay < limit; i++ {
		delay *= 2
	}
	if delay > limit {
		delay = limit
	}

	// Jitter of +/-20%.
	jitter := 0.8 + 0.4*mathrand.Float64()
	return time.Duration(float64(delay) * jitter)
}

// parseRetryAfter reads a Retry-After value in seconds or as an HTTP date,
// capped at 60 seconds.
func parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	var d time.Duration
	if secs, err := strconv.ParseFloat(value, 64); err == nil {
		d = time.Duration(secs * float64(time.Second))
	} else if t, err := http.ParseTime(value); err == nil {
		d = time.Until(t)
	} else {
		return 0, false
	}

	if d < 0 {
		d = 0
	}
	if d > maxRetryAfter {
		d = maxRetryAfter
	}
	return d, true
}

// sleepContext waits for d or until ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// newIdempotencyKey returns a random UUID v4.
func newIdempotencyKey() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; fall back to a
		// time-based value so the request can still be sent.
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
