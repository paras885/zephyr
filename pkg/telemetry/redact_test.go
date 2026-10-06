package telemetry

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestReplaceLogAttributeRedactsSecretsAndCredentials(t *testing.T) {
	for _, attribute := range []slog.Attr{
		slog.String("token", "worker-secret"),
		slog.String("client_secret", "oidc-secret"),
		slog.String("database_url", "postgres://user:password@db.example/zephyr"),
	} {
		redacted := replaceLogAttribute(nil, attribute).Value.String()
		if redacted != "[REDACTED]" {
			t.Errorf("attribute %q was not redacted: %q", attribute.Key, redacted)
		}
	}
	errorValue := replaceLogAttribute(nil, slog.Any("error", errors.New("connect postgres://user:password@db.example/zephyr failed"))).Value.String()
	if strings.Contains(errorValue, "password") || strings.Contains(errorValue, "user:") {
		t.Fatalf("error attribute contains database credentials: %s", errorValue)
	}
	message := redactLogString("dial postgres://user:password@db.example:5432/db and Bearer abc.def.secret")
	for _, secret := range []string{"password", "abc.def.secret", "user:"} {
		if strings.Contains(message, secret) {
			t.Errorf("redacted error contains %q: %s", secret, message)
		}
	}
}
