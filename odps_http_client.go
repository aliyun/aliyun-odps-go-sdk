package odps

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type HttpClient struct {
	// odps 账号
	Account
	// http超时时间，从tcp握手开始计时, 默认为0，即没有超时时间
	Timeout time.Duration
	_client *http.Client
}

func NewOdpsHttpClient(a Account) HttpClient {
	var client = HttpClient{
		Account: a,
	}

	var _ = client.client()

	return client
}

func NewOdpsHttpClientWithTimeout(a Account, timeout time.Duration) HttpClient {
	var client = HttpClient{
		Account: a,
	}

	var c = client.client()
	c.Timeout = timeout

	return client
}

func (client *HttpClient) client() *http.Client {
	if client._client != nil {
		return client._client
	}

	var transport = http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: false,
		DisableKeepAlives: true,
	}

	client._client = &http.Client{
		Transport: &transport,
		Timeout:   client.Timeout,
	}

	return client._client
}

func (client *HttpClient) NewRequest(method, resource string, body io.Reader) (*http.Request, error)  {
	var url = fmt.Sprintf (
		"%s/%s",
		strings.TrimRight(client.Endpoint(), "/"),
		strings.TrimLeft(resource, "/"))

	return http.NewRequest(method, url, body)
}

func (client *HttpClient) Do(req *http.Request) (*http.Response, error)  {
	req.Header.Set(HttpHeaderXOdpsUserAgent, UserAgentValue)
	gmtTime := time.Now().In(GMT).Format(time.RFC1123)
	req.Header.Set(HttpHeaderDate, gmtTime)

	client.SignRequest(req)

	fmt.Println(req.Header.Get("Authorization"))
	return client._client.Do(req)
}