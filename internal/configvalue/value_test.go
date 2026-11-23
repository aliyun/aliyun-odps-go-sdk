// Licensed under the Apache License, Version 2.0.

package configvalue

import (
	"strings"
	"testing"
	"time"
)

func TestEndpointValidation(t *testing.T) {
	for _, value := range []string{"http://example.test:80/api", "https://example.test/api"} {
		u, err := Endpoint(value, "endpoint")
		if err != nil || u.String() != value {
			t.Fatalf("valid endpoint %q: %v", value, err)
		}
	}
	for _, value := range []string{
		"http://%", "/api", "ftp://example.test", "http://:80",
		"http://example.test:65536", "http://example.test:0", "http://example.test?",
		"http://example.test?project=p", "http://example.test#fragment",
		"https://test-user:test-secret@example.test",
	} {
		if _, err := Endpoint(value, "endpoint"); err == nil || strings.Contains(err.Error(), "test-secret") {
			t.Errorf("expected a redacted endpoint validation error for a bad input")
		}
	}
}

func TestTimeoutSeconds(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{"", time.Minute},
		{"0", 0},
		{"12", 12 * time.Second},
	} {
		got, err := TimeoutSeconds(tt.value, "timeout", time.Minute)
		if err != nil || got != tt.want {
			t.Errorf("TimeoutSeconds(%q) = %v, %v; want %v", tt.value, got, err, tt.want)
		}
	}
	for _, unacceptable := range []string{"-1", "1.5", "2147483648", "nonnumeric"} {
		if _, err := TimeoutSeconds(unacceptable, "timeout", time.Minute); err == nil {
			t.Errorf("invalid timeout %q accepted", unacceptable)
		}
	}
}
