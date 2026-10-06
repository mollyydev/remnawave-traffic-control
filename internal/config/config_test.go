package config

import (
	"os"
	"testing"
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
