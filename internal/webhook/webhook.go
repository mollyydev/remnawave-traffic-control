package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	enabled bool
	url     string
	secret  string
	http    *http.Client
}

func New(enabled bool, url, secret string, timeout time.Duration) *Client {
	return &Client{
		enabled: enabled,
		url:     url,
		secret:  secret,
		http: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				// Do not follow redirects: this client carries an authentication
				// signature which must never be sent to an unintended host.
				return http.ErrUseLastResponse
			},
		},
	}
}

func (c *Client) Enabled() bool { return c.enabled }

func (c *Client) Send(ctx context.Context, eventID string, payload []byte) error {
	if !c.enabled {
		return nil
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(c.secret))
	_, _ = mac.Write([]byte(ts))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(payload)
	signature := hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Whitelists-Event-ID", eventID)
	req.Header.Set("X-Whitelists-Event", "whitelist.exhausted")
	req.Header.Set("X-Whitelists-Timestamp", ts)
	req.Header.Set("X-Whitelists-Signature", "sha256="+signature)
	req.Header.Set("User-Agent", "whitelists-service/1")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return &HTTPError{StatusCode: resp.StatusCode, Body: msg}
	}
	return nil
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("webhook status=%d body=%s", e.StatusCode, e.Body)
}

// IsPermanent reports webhook responses that are unlikely to become valid
// without an operator/request change. Retryable exceptions include 408, 409,
// 425 and 429; 5xx and transport errors are retried by the outbox.
func IsPermanent(err error) bool {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	if httpErr.StatusCode < 400 || httpErr.StatusCode >= 500 {
		return false
	}
	switch httpErr.StatusCode {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooEarly, http.StatusTooManyRequests:
		return false
	default:
		return true
	}
}
