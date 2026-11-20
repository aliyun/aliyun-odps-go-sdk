package common

import (
	"context"
	"strings"
	"time"
)

func QuoteString(str string) string {
	str = strings.ReplaceAll(str, "'", "\\'")
	return "'" + str + "'"
}

func QuoteRef(ref string) string {
	return "`" + ref + "`"
}

// StringToBool 将字符串转换为布尔值，不合法时返回 false
func StringToBool(s string) bool {
	return strings.ToLower(s) == "true"
}

// SleepContext waits for the delay or for the caller to cancel.
func SleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	return ctx.Err()
}
