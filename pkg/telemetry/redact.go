package telemetry

import (
	"log/slog"
	"regexp"
	"strings"
)

var (
	credentialURL = regexp.MustCompile(`(?i)\b(postgres(?:ql)?|amqps?)://[^/\s@]+@`)
	bearerValue   = regexp.MustCompile(`(?i)(bearer\s+)[a-z0-9._~+/-]+=*`)
)

func replaceLogAttribute(_ []string, attribute slog.Attr) slog.Attr {
	key := strings.ToLower(attribute.Key)
	switch key {
	case "authorization", "token", "access_token", "refresh_token", "password", "secret", "client_secret", "dsn", "database_url", "amqp_url":
		attribute.Value = slog.StringValue("[REDACTED]")
		return attribute
	}
	if attribute.Value.Kind() == slog.KindString {
		attribute.Value = slog.StringValue(redactLogString(attribute.Value.String()))
	} else if value, ok := attribute.Value.Any().(error); ok {
		attribute.Value = slog.StringValue(redactLogString(value.Error()))
	}
	return attribute
}

func redactLogString(message string) string {
	message = credentialURL.ReplaceAllString(message, "$1://[REDACTED]@")
	return bearerValue.ReplaceAllString(message, "${1}[REDACTED]")
}
