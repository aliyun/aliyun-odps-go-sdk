package utils

import (
	"testing"
	"time"
)

func TestToString(t *testing.T) {
	for _, tc := range []struct {
		input interface{}
		want  string
	}{
		{"hello", "hello"},
		{sp("hello"), "hello"},
		{-1, "-1"},
		{int8(-128), "-128"},
		{int16(-32768), "-32768"},
		{int32(-2147483648), "-2147483648"},
		{int64(-27), "-27"},
		{uint(1), "1"},
		{uint8(255), "255"},
		{uint16(65535), "65535"},
		{uint32(27), "27"},
		{uint64(18446744073709551615), "18446744073709551615"},
		{float32(1.25), "1.25"},
		{float64(-3.5), "-3.5"},
		{true, "true"},
		{[]int{1, 2}, "[1 2]"},
	} {
		if got := ToString(tc.input); got == nil || *got != tc.want {
			t.Errorf("ToString(%#v) = %v, want %q", tc.input, got, tc.want)
		}
	}
	if ToString(nil) != nil {
		t.Error("ToString(nil) must return nil")
	}
	if ToString((*string)(nil)) != nil {
		t.Error("ToString on a typed nil pointer must return nil")
	}
}

func sp(s string) *string { return &s }

// Opaque pagination tokens (e.g. nextPageToken) may contain reserved
// characters such as '!'. The ODPS server builds its signing canonical
// resource from the raw, unescaped query values, so the SDK must do the
// same. Escaping the value here produces a signature mismatch and a 401
// on the next page. See aliyun-odps-openapi-sdk issue #6.
func TestBuildCanonicalResourcePreservesRawQueryValues(t *testing.T) {
	resource := "/api/catalog/v1alpha/projects/example_project/schemas/default/tables"
	params := map[string]*string{
		"pageToken": sp("opaque!cursor"),
	}

	got := buildCanonicalResource(sp(resource), params)
	want := resource + "?pageToken=opaque!cursor"

	if got != want {
		t.Errorf("buildCanonicalResource() = %q, want %q", got, want)
	}
}

func TestBuildCanonicalResourceSortsKeysAndOmitsEmptyValues(t *testing.T) {
	params := map[string]*string{
		"pageToken":   sp("a!b"),
		"maxResults":  sp("10"),
		"emptyFilter": sp(""),
		"flag":        nil,
	}

	got := buildCanonicalResource(sp("/path"), params)
	want := "/path?emptyFilter&flag&maxResults=10&pageToken=a!b"

	if got != want {
		t.Errorf("buildCanonicalResource() = %q, want %q", got, want)
	}
}

func TestCanonicalSigningHeaders(t *testing.T) {
	resource := sp("/catalog")
	headers := map[string]*string{
		"Content-MD5":        sp("md5"),
		"Content-Type":       sp("application/json"),
		"DATE":               sp("date"),
		"X-Odps-Trace":       sp("header"),
		"x-odps-null-header": nil,
		"X-Unrelated":        sp("omit"),
		"":                   sp("omit"),
	}
	params := map[string]*string{
		"range":             sp("open"),
		"x-odps-trace":      sp("query"),
		"x-odps-null-param": nil,
	}
	want := "POST\nmd5\napplication/json\ndate\nx-odps-null-header:\nx-odps-null-param:\nx-odps-trace:query\n" +
		"/catalog?range=open&x-odps-null-param&x-odps-trace=query"
	if got := *BuildCanonicalString(sp("POST"), resource, params, headers); got != want {
		t.Errorf("canonical string = %q, want %q", got, want)
	}
	for _, params := range []map[string]*string{nil, {}} {
		if got := *BuildCanonicalString(sp("GET"), resource, params, nil); got != "GET\n\n\n/catalog" {
			t.Errorf("canonical string without metadata = %q", got)
		}
	}
}

func TestSignature(t *testing.T) {
	// RFC 2202's HMAC-SHA1 vector with the ODPS authorization prefix.
	got := *GetSignature(sp("what do ya want for nothing?"), sp("fixture-ak"), sp("Jefe"))
	const want = "ODPS fixture-ak:7/zfauXrL6LSdBbV8YTfnCWafHk="
	if got != want {
		t.Errorf("authorization = %q, want %q", got, want)
	}
}

func TestAPITimestamp(t *testing.T) {
	start := time.Now().Truncate(time.Second)
	got := *GetApiTimestamp()
	parsed, err := time.Parse(time.RFC1123, got)
	if err != nil || parsed.Location().String() != "GMT" {
		t.Fatalf("timestamp %q must be RFC1123 in GMT, parse error %v", got, err)
	}
	if parsed.Before(start) || parsed.After(time.Now()) {
		t.Errorf("timestamp %q falls outside the execution window", got)
	}
}
