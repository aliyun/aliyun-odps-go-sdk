package utils

import (
	"fmt"
	"testing"
)

func TestToString(t *testing.T) {
	value := "hello"
	var ptr *string
	ptr = &value

	t.Log(fmt.Sprintf("%v", ptr)) // 0x000
	t.Log(*ToString(ptr))         // hello
	t.Log(*ToString(value))       // hello
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
	}

	got := buildCanonicalResource(sp("/path"), params)
	want := "/path?emptyFilter&maxResults=10&pageToken=a!b"

	if got != want {
		t.Errorf("buildCanonicalResource() = %q, want %q", got, want)
	}
}
