package api

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGBToBytes(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		want int64
	}{
		{name: "zero", in: 0, want: 0},
		{name: "one", in: 1, want: 1 << 30},
		{name: "half", in: 0.5, want: 1 << 29},
		{name: "decimal", in: 1.25, want: 1342177280},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gbToBytes(tt.in)
			if err != nil {
				t.Fatalf("gbToBytes() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("gbToBytes(%v) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestGBToBytesRejectsInvalid(t *testing.T) {
	for _, value := range []float64{-1, math.NaN(), math.Inf(1)} {
		if _, err := gbToBytes(value); err == nil {
			t.Fatalf("gbToBytes(%v) returned nil error", value)
		}
	}
}

func TestRemainingBytes(t *testing.T) {
	if got := remainingBytes(100, 40); got != 60 {
		t.Fatalf("remainingBytes() = %d, want 60", got)
	}
	if got := remainingBytes(100, 120); got != 0 {
		t.Fatalf("remainingBytes() = %d, want 0", got)
	}
	if got := remainingBytes(0, 100); got != 0 {
		t.Fatalf("remainingBytes() unlimited = %d, want 0", got)
	}
}

func TestNullableIntDistinguishesOmittedAndNull(t *testing.T) {
	var omitted updateRequest
	if err := decodeJSON(requestWithJSON(`{}`), &omitted); err != nil {
		t.Fatalf("decode omitted: %v", err)
	}
	if omitted.LimitResetDays.Set {
		t.Fatal("omitted limit_reset must not be marked as set")
	}

	var explicitNull updateRequest
	if err := decodeJSON(requestWithJSON(`{"limit_reset":null}`), &explicitNull); err != nil {
		t.Fatalf("decode null: %v", err)
	}
	if !explicitNull.LimitResetDays.Set || explicitNull.LimitResetDays.Value != nil {
		t.Fatal("explicit null must be marked as set with nil value")
	}

	var days updateRequest
	if err := decodeJSON(requestWithJSON(`{"limit_reset":30}`), &days); err != nil {
		t.Fatalf("decode days: %v", err)
	}
	if !days.LimitResetDays.Set || days.LimitResetDays.Value == nil || *days.LimitResetDays.Value != 30 {
		t.Fatal("integer limit_reset was not decoded correctly")
	}
}

func TestAuthMiddleware(t *testing.T) {
	s := &Server{apiKey: `"my-super-secret-key-12345678901234567890"`} // quotes in config
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := s.auth(dummyHandler)

	tests := []struct {
		name       string
		headerKey  string
		headerVal  string
		authHeader string
		queryParam string
		wantStatus int
	}{
		{
			name:       "exact X-API-Key without quotes",
			headerKey:  "X-API-Key",
			headerVal:  "my-super-secret-key-12345678901234567890",
			wantStatus: http.StatusOK,
		},
		{
			name:       "X-API-Key with quotes and spaces",
			headerKey:  "X-API-Key",
			headerVal:  `  "my-super-secret-key-12345678901234567890" `,
			wantStatus: http.StatusOK,
		},
		{
			name:       "Authorization Bearer",
			authHeader: "Bearer my-super-secret-key-12345678901234567890",
			wantStatus: http.StatusOK,
		},
		{
			name:       "Authorization ApiKey",
			authHeader: "ApiKey my-super-secret-key-12345678901234567890",
			wantStatus: http.StatusOK,
		},
		{
			name:       "query param api_key",
			queryParam: "api_key=my-super-secret-key-12345678901234567890",
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong key",
			headerKey:  "X-API-Key",
			headerVal:  "wrong-secret-key",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "empty key",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := "/v1/users/1"
			if tt.queryParam != "" {
				target += "?" + tt.queryParam
			}
			req := httptest.NewRequest(http.MethodGet, target, nil)
			if tt.headerKey != "" {
				req.Header.Set(tt.headerKey, tt.headerVal)
			}
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("auth returned status %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

func TestNormalizePathMiddleware(t *testing.T) {
	s := &Server{}
	var recordedPath string
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	handler := s.normalizePath(dummyHandler)

	tests := []struct {
		inputPath string
		wantPath  string
	}{
		{inputPath: "/traffic/v1/users/1", wantPath: "/v1/users/1"},
		{inputPath: "/traffic/v1/users/1/trafic", wantPath: "/v1/users/1/trafic"},
		{inputPath: "/traffic/healthz", wantPath: "/healthz"},
		{inputPath: "/traffic", wantPath: "/"},
		{inputPath: "/v1/users/1", wantPath: "/v1/users/1"},
		{inputPath: "/healthz", wantPath: "/healthz"},
	}

	for _, tt := range tests {
		t.Run(tt.inputPath, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.inputPath, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if recordedPath != tt.wantPath {
				t.Fatalf("path = %q, want %q", recordedPath, tt.wantPath)
			}
		})
	}
}

func requestWithJSON(body string) *http.Request {
	r := httptest.NewRequest("POST", "/", bytes.NewBufferString(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}
