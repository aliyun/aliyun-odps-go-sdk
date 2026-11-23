package restclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
)

// ProxyFromEnvironment caches configuration, so use a fresh process.
func TestRestClientEnvironmentProxy(t *testing.T) {
	if os.Getenv("ODPS_TEST_PROXY_CHILD") == "1" {
		client := NewOdpsRestClient(MockAccount{}, "http://unresolvable.invalid/api")
		req, err := client.NewRequest(http.MethodGet, "projects/test", nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil || string(body) != "proxy-ok" {
			t.Fatalf("proxy response: %q, %v", body, err)
		}
		return
	}
	hits := make(chan string, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits <- r.URL.String()
		io.WriteString(w, "proxy-ok")
	}))
	defer proxy.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRestClientEnvironmentProxy$")
	cmd.Env = append(os.Environ(), "ODPS_TEST_PROXY_CHILD=1", "HTTP_PROXY="+proxy.URL,
		"http_proxy=", "HTTPS_PROXY=", "https_proxy=", "NO_PROXY=", "no_proxy=", "REQUEST_METHOD=")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("proxy child: %v\n%s", err, out)
	}
	select {
	case target := <-hits:
		if target != "http://unresolvable.invalid/api/projects/test" {
			t.Fatalf("unexpected proxy target %s", target)
		}
	default:
		t.Fatal("request did not reach proxy")
	}
}
