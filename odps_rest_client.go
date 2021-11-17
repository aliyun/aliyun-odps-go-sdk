package odps

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Todo 请求方法需要重构，加入header参数

type RestClient struct {
	// odps 账号
	Account
	// http超时时间，从tcp握手开始计时, 默认为0，即没有超时时间
	Timeout        time.Duration
	_client        *http.Client
	defaultProject string
	endpoint       string
}

func NewOdpsHttpClient(a Account, endpoint string) RestClient {
	var client = RestClient{
		Account: a,
		endpoint: endpoint,
	}

	var _ = client.client()

	return client
}

func NewOdpsHttpClientWithTimeout(a Account, endpoint string, timeout time.Duration) RestClient {
	var client = RestClient{
		Account: a,
		endpoint: endpoint,
	}

	var c = client.client()
	c.Timeout = timeout

	return client
}


func LoadEndpointFromEnv() string {
	endpoint, _:= os.LookupEnv("odps_endpoint")
	return endpoint
}

func (client *RestClient) setDefaultProject(projectName string) {
	client.defaultProject = projectName
}

func (client *RestClient) Endpoint() string {
	return client.endpoint
}

func (client *RestClient) client() *http.Client {
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
		DisableKeepAlives: false,
	}

	client._client = &http.Client{
		Transport: &transport,
		Timeout:   client.Timeout,
	}

	return client._client
}

func (client *RestClient) NewRequest(method, resource string, body io.Reader) (*http.Request, error) {
	var url = fmt.Sprintf(
		"%s/%s",
		strings.TrimRight(client.Endpoint(), "/"),
		strings.TrimLeft(resource, "/"))

	return http.NewRequest(method, url, body)
}

func (client *RestClient) NewRequestWithUrlQuery(method, resource string, body io.Reader, queryArgs url.Values) (*http.Request, error) {
	req, err := client.NewRequest(method, resource, body)
	if err != nil {
		return nil, err
	}

	if queryArgs != nil {
		req.URL.RawQuery = queryArgs.Encode()
	}

	return req, nil
}

func (client *RestClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set(HttpHeaderXOdpsUserAgent, UserAgentValue)
	gmtTime := time.Now().In(GMT).Format(time.RFC1123)
	req.Header.Set(HttpHeaderDate, gmtTime)
	query := req.URL.Query()

	if !query.Has("curr_project") && client.defaultProject != "" {
		query.Set("curr_project", client.defaultProject)
	}
	req.URL.RawQuery = query.Encode()

	client.SignRequest(req, client.endpoint)

	return client._client.Do(req)
}

func (client *RestClient) DoWithParseFunc(req *http.Request, parseFunc func(res *http.Response) error) error {
	return client.DoWithParseRes(req, func(res *http.Response) error {
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return NewHttpNotOk(res)
		}

		if parseFunc == nil {
			return nil
		}

		return parseFunc(res)
	})
}

func (client *RestClient) DoWithParseRes(req *http.Request, parseFunc func(res *http.Response) error) error {
	res, err := client.Do(req)

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalf("close http error, url=%s", req.URL.String())
		}
	}(res.Body)

	if err != nil {
		return err
	}

	if parseFunc == nil {
		return nil
	}

	return parseFunc(res)
}

func (client *RestClient) DoWithModel(req *http.Request, model interface{}) error {
	parseFunc := func(res *http.Response) error {
		decoder := xml.NewDecoder(res.Body)

		return decoder.Decode(model)
	}

	return client.DoWithParseFunc(req, parseFunc)
}

func (client *RestClient) GetWithModel(resource string, queryArgs url.Values, model interface{}) error {
	req, err := client.NewRequestWithUrlQuery(GetMethod, resource, nil, queryArgs)
	if err != nil {
		return err
	}

	return client.DoWithModel(req, model)
}

func (client *RestClient) GetWithParseFunc(resource string, queryArgs url.Values, parseFunc func(res *http.Response) error) error {
	req, err := client.NewRequestWithUrlQuery(GetMethod, resource, nil, queryArgs)
	if err != nil {
		return err
	}

	return client.DoWithParseFunc(req, parseFunc)
}

func (client *RestClient) PutWithParseFunc(resource string, queryArgs url.Values, body io.Reader, parseFunc func(res *http.Response) error) error {
	req, err := client.NewRequestWithUrlQuery(PutMethod, resource, body, queryArgs)
	if err != nil {
		return err
	}

	return client.DoWithParseFunc(req, parseFunc)
}

func (client *RestClient) DoXmlWithParseFunc(
	method string,
	resource string,
	queryArgs url.Values,
	bodyModel interface{},
	parseFunc func(res *http.Response) error) error {

	bodyXml, err := xml.Marshal(bodyModel)

	if err != nil {
		return err
	}

	req, err := client.NewRequestWithUrlQuery(method, resource, bytes.NewReader(bodyXml), queryArgs)
	req.Header.Set(HttpHeaderContentType, XMLContentType)

	if err != nil {
		return err
	}

	return client.DoWithParseFunc(req, parseFunc)
}

func (client *RestClient) DoXmlWithParseRes(
	method string,
	resource string,
	queryArgs url.Values,
	bodyModel interface{},
	parseFunc func(res *http.Response) error) error {

	bodyXml, err := xml.Marshal(bodyModel)

	if err != nil {
		return err
	}

	req, err := client.NewRequestWithUrlQuery(method, resource, bytes.NewReader(bodyXml), queryArgs)
	req.Header.Set(HttpHeaderContentType, XMLContentType)

	if err != nil {
		return err
	}

	return client.DoWithParseRes(req, parseFunc)
}

func (client *RestClient) DoXmlWithModel(
	method string,
	resource string,
	queryArgs url.Values,
	bodyModel interface{},
	resModel interface{}) error {

	parseFunc := func(res *http.Response) error {
		decoder := xml.NewDecoder(res.Body)

		return decoder.Decode(resModel)
	}

	return client.DoXmlWithParseFunc(method, resource, queryArgs, bodyModel, parseFunc)
}
