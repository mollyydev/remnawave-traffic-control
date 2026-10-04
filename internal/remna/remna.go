package remna

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL      string
	token        string
	http         *http.Client
	maxRetries   int
	retryBackoff time.Duration
}

type User struct {
	ID                   int64         `json:"id"`
	Username             string        `json:"username"`
	ActiveInternalSquads []ActiveSquad `json:"activeInternalSquads"`
}

type ActiveSquad struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

type updateUserRequest struct {
	ID                   int64    `json:"id"`
	ActiveInternalSquads []string `json:"activeInternalSquads"`
}

func New(baseURL, token string, timeout time.Duration, maxRetries int) *Client {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				// Never forward the Remnawave bearer token to another host.
				return http.ErrUseLastResponse
			},
		},
		maxRetries:   maxRetries,
		retryBackoff: 250 * time.Millisecond,
	}
}

func (c *Client) GetUser(ctx context.Context, id int64) (User, error) {
	body, err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/users/%d", id), nil)
	if err != nil {
		return User{}, err
	}
	var envelope struct {
		Response User `json:"response"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return User{}, fmt.Errorf("decode Remnawave user: %w", err)
	}
	if envelope.Response.ID == 0 {
		return User{}, errors.New("Remnawave returned an empty user response")
	}
	return envelope.Response, nil
}

// SyncWhitelistSquad makes the configured whitelist squad membership match
// wantPresent. It is idempotent: if the current state is already correct, no
// PATCH is sent.
// DropUserConnections asks Remnawave to terminate the user's current connections.
// Remnawave processes this as a background operation and returns 202 on success.
func (c *Client) DropUserConnections(ctx context.Context, userID int64) error {
	payload, err := json.Marshal(map[string]any{
		"dropBy": map[string]any{
			"userIds": []int64{userID},
		},
	})
	if err != nil {
		return fmt.Errorf("encode connection drop request: %w", err)
	}
	_, err = c.do(ctx, http.MethodPost, "/api/connections/drop", payload)
	return err
}

func (c *Client) SyncWhitelistSquad(ctx context.Context, userID int64, squadID string, wantPresent bool) error {
	u, err := c.GetUser(ctx, userID)
	if err != nil {
		return err
	}

	has := false
	squads := make([]string, 0, len(u.ActiveInternalSquads)+1)
	for _, squad := range u.ActiveInternalSquads {
		if strings.EqualFold(squad.UUID, squadID) {
			has = true
			continue
		}
		squads = append(squads, squad.UUID)
	}
	if wantPresent {
		if has {
			return nil
		}
		squads = append(squads, squadID)
	} else {
		if !has {
			return nil
		}
	}

	payload, err := json.Marshal(updateUserRequest{ID: u.ID, ActiveInternalSquads: squads})
	if err != nil {
		return fmt.Errorf("encode Remnawave update: %w", err)
	}
	_, err = c.do(ctx, http.MethodPatch, "/api/users", payload)
	return err
}

func (c *Client) do(ctx context.Context, method, path string, payload []byte) ([]byte, error) {
	var lastErr error
	attempts := c.maxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := c.retryBackoff << (attempt - 1)
			if delay > 4*time.Second {
				delay = 4 * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}

		body, err := c.doOnce(ctx, method, path, payload)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !isRetryable(err) {
			break
		}
	}
	return nil, lastErr
}

func (c *Client) doOnce(ctx context.Context, method, path string, payload []byte) ([]byte, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body := strings.TrimSpace(string(respBody))
		if len(body) > 4096 {
			body = body[:4096] + "..."
		}
		return nil, &HTTPError{StatusCode: resp.StatusCode, Body: body}
	}
	return respBody, nil
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("Remnawave API status=%d", e.StatusCode)
	}
	return fmt.Sprintf("Remnawave API status=%d body=%s", e.StatusCode, e.Body)
}

// IsNotFound reports whether the Remnawave API returned HTTP 404.
// A missing panel user is a terminal condition for whitelist enforcement;
// retrying it forever would only fill the outbox with permanently stale work.
func IsNotFound(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound
}

// IsPermanent reports errors that should not be retried by the outbox. Most
// 4xx responses are operator/configuration/data errors rather than transient
// failures. Conflict/timeout/too-early/rate-limit are kept retryable.
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

func isRetryable(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= 500
	}
	return true
}
