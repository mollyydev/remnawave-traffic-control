package config

import (
	"os"
	"testing"
	"time"
)

func TestGetHTTPAddr(t *testing.T) {
	tests := []struct {
		name     string
		envKey   string
		envVal   string
		expected string
	}{
		{
			name:     "default when unset",
			expected: ":8080",
		},
		{
			name:     "HTTP_ADDR with colon",
			envKey:   "HTTP_ADDR",
			envVal:   ":14488",
			expected: ":14488",
		},
		{
			name:     "http_adr with colon (user typo alias)",
			envKey:   "http_adr",
			envVal:   ":14488",
			expected: ":14488",
		},
		{
			name:     "HTTP_ADR with colon",
			envKey:   "HTTP_ADR",
			envVal:   ":14488",
			expected: ":14488",
		},
		{
			name:     "http_addr with colon",
			envKey:   "http_addr",
			envVal:   ":14488",
			expected: ":14488",
		},
		{
			name:     "HTTP_PORT without colon",
			envKey:   "HTTP_PORT",
			envVal:   "14488",
			expected: ":14488",
		},
		{
			name:     "PORT without colon",
			envKey:   "PORT",
			envVal:   "9090",
			expected: ":9090",
		},
		{
			name:     "http_adr without colon",
			envKey:   "http_adr",
			envVal:   "14488",
			expected: ":14488",
		},
		{
			name:     "host and port",
			envKey:   "HTTP_ADDR",
			envVal:   "127.0.0.1:14488",
			expected: "127.0.0.1:14488",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all port-related env vars first
			for _, k := range []string{"HTTP_ADDR", "http_addr", "HTTP_ADR", "http_adr", "HTTP_PORT", "http_port", "PORT", "port"} {
				_ = os.Unsetenv(k)
			}
			if tt.envKey != "" {
				_ = os.Setenv(tt.envKey, tt.envVal)
				defer os.Unsetenv(tt.envKey)
			}
			got := getHTTPAddr()
			if got != tt.expected {
				t.Fatalf("getHTTPAddr() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "30d", want: 30 * 24 * time.Hour},
		{in: "30D", want: 30 * 24 * time.Hour},
		{in: "1d", want: 24 * time.Hour},
		{in: "7days", want: 7 * 24 * time.Hour},
		{in: "1day", want: 24 * time.Hour},
		{in: "0.5d", want: 12 * time.Hour},
		{in: "15s", want: 15 * time.Second},
		{in: "2m", want: 2 * time.Minute},
		{in: "24h", want: 24 * time.Hour},
		{in: "0s", want: 0},
		{in: "invalid", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseDuration(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseDuration(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("parseDuration(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
