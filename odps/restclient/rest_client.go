// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restclient

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
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

// Todo 请求方法需要重构，加入header参数
const (
	DefaultHttpTimeout          = 60
	DefaultTcpConnectionTimeout = 30
)

type RestClient struct {
	account.Account
	// It is the total time from the tcp connection to http response.
	// the default value is 0, represents no timeout
	HttpTimeout          time.Duration
	TcpConnectionTimeout time.Duration
	DnsCacheExpireTime   time.Duration
	DisableCompression   bool
	// DisableKeepAlives closes the TCP connection as soon as a response is done
	// with, instead of pooling it. Callers that build a RestClient per request
	// and get nothing out of pooling use it so that idle connections - and the
	// goroutines serving them - do not pile up.
	DisableKeepAlives bool
	// _slot holds the lazily created http.Client and is a pointer on purpose:
	// RestClient values get copied all over the SDK ("client := odpsIns.restClient"),
	// and a per-copy http.Client means a per-copy http.Transport, i.e. one pooled
	// connection (plus its goroutines) leaked per request. Sharing the slot makes
	// a RestClient and all of its copies use one connection pool.
	_slot          *clientSlot
	defaultProject string
	currentSchema  string
	endpoint       string
	userAgent      string
}

// clientSlot is the state shared by one RestClient and every copy of it.
// The mutex lives here rather than on RestClient so copying the struct never
// copies a lock.
type clientSlot struct {
	mu    sync.Mutex
	built bool
	// settings is the snapshot the current client was built from, so a caller
	// that changes a timeout on a live RestClient still gets it applied instead
	// of silently keeping the client built by an earlier request.
	settings clientSettings
	client   *http.Client
}

// clientSettings are the fields that decide how the http.Client is built.
type clientSettings struct {
	endpoint             string
	httpTimeout          time.Duration
	tcpConnectionTimeout time.Duration
	dnsCacheExpireTime   time.Duration
	disableCompression   bool
	disableKeepAlives    bool
}

func NewOdpsRestClient(a account.Account, endpoint string) RestClient {
	client := RestClient{
		Account:              a,
		endpoint:             endpoint,
		HttpTimeout:          DefaultHttpTimeout * time.Second,
		TcpConnectionTimeout: DefaultTcpConnectionTimeout * time.Second,
		DnsCacheExpireTime:   time.Duration(DefaultDNSCacheExpireTime) * time.Second,
		DisableCompression:   true,
		_slot:                &clientSlot{},
	}

	return client
}

func LoadEndpointFromEnv() string {
	endpoint, _ := os.LookupEnv("odps_endpoint")
	return endpoint
}

func (client *RestClient) SetDefaultProject(projectName string) {
	client.defaultProject = projectName
}

func (client *RestClient) SetCurrentSchema(schemaName string) {
	client.currentSchema = schemaName
}

func (client *RestClient) SetUserAgent(userAgent string) {
	client.userAgent = userAgent
}

func (client *RestClient) UserAgent() string {
	if client.userAgent != "" {
		return client.userAgent
	}

	return common.UserAgentValue
}

// CloseIdleConnections closes the keep-alive connections that this client
// pooled but is not using. A RestClient owns its http.Transport, so a caller
// that is done with it (for example a database/sql connection being discarded)
// should call this to release the pooled connections and the goroutines that
// serve them. It is a no-op if no client has been created yet.
func (client *RestClient) CloseIdleConnections() {
	if client._slot == nil {
		return
	}

	slot := client._slot
	slot.mu.Lock()
	built := slot.client
	slot.mu.Unlock()

	if built != nil {
		built.CloseIdleConnections()
	}
}

func (client *RestClient) Endpoint() string {
	return client.endpoint
}

func (client *RestClient) client() *http.Client {
	if client._slot == nil {
		// A RestClient not built by NewOdpsRestClient: fall back to per-copy
		// state, which is what this code did before.
		client._slot = &clientSlot{}
	}

	slot := client._slot
	want := client.settings()

	slot.mu.Lock()
	defer slot.mu.Unlock()

	if slot.built && slot.settings == want {
		return slot.client
	}

	if slot.client != nil {
		// Settings changed on a live client: drop the old connection pool so it
		// does not linger behind the replacement.
		slot.client.CloseIdleConnections()
	}

	resolver := NewResolver(int64(want.dnsCacheExpireTime) / int64(time.Second))

	dialer := Dialer{
		Resolver: resolver,
		Dialer: net.Dialer{
			Timeout:   want.tcpConnectionTimeout,
			KeepAlive: 30 * time.Second,
		},
	}

	transport := http.Transport{
		Proxy:              http.ProxyFromEnvironment,
		DialContext:        dialer.DialContext,
		ForceAttemptHTTP2:  false,
		DisableCompression: want.disableCompression,
		DisableKeepAlives:  want.disableKeepAlives,
	}

	slot.client = &http.Client{
		Transport: &transport,
		Timeout:   want.httpTimeout,
	}
	slot.settings = want
	slot.built = true

	return slot.client
}

// settings snapshots everything that influences the http.Client. It is a value
// type on purpose: comparing it tells client() whether the pooled transport is
// still the one the caller asked for.
func (client *RestClient) settings() clientSettings {
	return clientSettings{
		endpoint:             client.endpoint,
		httpTimeout:          client.HttpTimeout,
		tcpConnectionTimeout: client.TcpConnectionTimeout,
		dnsCacheExpireTime:   client.DnsCacheExpireTime,
		disableCompression:   client.DisableCompression,
		disableKeepAlives:    client.DisableKeepAlives,
	}
}

func (client *RestClient) NewRequest(method, resource string, body io.Reader) (*http.Request, error) {
	urlStr := fmt.Sprintf(
		"%s/%s",
		strings.TrimRight(client.Endpoint(), "/"),
		strings.TrimLeft(resource, "/"))

	req, err := http.NewRequest(method, urlStr, body)
	return req, errors.WithStack(err)
}

func (client *RestClient) NewRequestWithUrlQuery(method, resource string, body io.Reader, queryArgs url.Values) (*http.Request, error) {
	req, err := client.NewRequest(method, resource, body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if queryArgs != nil {
		req.URL.RawQuery = queryArgs.Encode()
	}

	return req, nil
}

func (client *RestClient) NewRequestWithParamsAndHeaders(method, resource string, body io.Reader, params url.Values, headers map[string]string) (*http.Request, error) {
	req, err := client.NewRequestWithUrlQuery(method, resource, body, params)
	if err != nil {
		return nil, err
	}
	if headers != nil {
		for name, value := range headers {
			req.Header.Set(name, value)
		}
	}
	return req, nil
}

func (client *RestClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set(common.HttpHeaderUserAgent, common.UserAgentValue)
	req.Header.Set(common.HttpHeaderXOdpsUserAgent, client.UserAgent())
	gmtTime := time.Now().In(common.GMT).Format(time.RFC1123)
	req.Header.Set(common.HttpHeaderDate, gmtTime)
	query := req.URL.Query()

	_, ok := query["current_project"]
	// in go1.17, 下面的语句应该这样写：if !query.Has("curr_project") && client.defaultProject != "" {
	// 但values.Has方法是在go1.17才引入的，为了兼容go1.15，所以不用Has方法
	if !ok && client.defaultProject != "" {
		query.Set("curr_project", client.defaultProject)
	}
	req.URL.RawQuery = query.Encode()

	err := client.SignRequest(req, client.endpoint)
	if err != nil {
		return nil, err
	}

	res, err := client.client().Do(req)
	if err != nil {
		return res, errors.WithStack(err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return res, errors.WithStack(NewHttpNotOk(res))
	}
	return res, nil
}

func (client *RestClient) DoWithParseFunc(req *http.Request, parseFunc func(res *http.Response) error) error {
	return client.DoWithParseRes(req, func(res *http.Response) error {
		if parseFunc == nil {
			return nil
		}
		return errors.WithStack(parseFunc(res))
	})
}

func (client *RestClient) DoWithParseRes(req *http.Request, parseFunc func(res *http.Response) error) error {
	res, err := client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalf("close http error, url=%s", req.URL.String())
		}
	}(res.Body)

	if parseFunc == nil {
		return nil
	}

	return errors.WithStack(parseFunc(res))
}

func (client *RestClient) DoWithModel(req *http.Request, model interface{}) error {
	parseFunc := func(res *http.Response) error {
		decoder := xml.NewDecoder(res.Body)

		return errors.WithStack(decoder.Decode(model))
	}

	return errors.WithStack(client.DoWithParseFunc(req, parseFunc))
}

func (client *RestClient) GetWithModel(resource string, queryArgs url.Values, headers map[string]string, model interface{}) error {
	req, err := client.NewRequestWithParamsAndHeaders(common.HttpMethod.GetMethod, resource, nil, queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(client.DoWithModel(req, model))
}

func (client *RestClient) GetWithParseFunc(resource string, queryArgs url.Values, headers map[string]string, parseFunc func(res *http.Response) error) error {
	req, err := client.NewRequestWithParamsAndHeaders(common.HttpMethod.GetMethod, resource, nil, queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(client.DoWithParseFunc(req, parseFunc))
}

func (client *RestClient) PutWithParseFunc(resource string, queryArgs url.Values, body io.Reader, parseFunc func(res *http.Response) error) error {
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.PutMethod, resource, body, queryArgs)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(client.DoWithParseFunc(req, parseFunc))
}

func (client *RestClient) DoXmlWithParseFunc(
	method string,
	resource string,
	queryArgs url.Values,
	headers map[string]string,
	bodyModel interface{},
	parseFunc func(res *http.Response) error,
) error {
	bodyXml, err := xml.Marshal(bodyModel)
	if err != nil {
		return errors.WithStack(err)
	}

	req, err := client.NewRequestWithUrlQuery(method, resource, bytes.NewReader(bodyXml), queryArgs)
	req.Header.Set(common.HttpHeaderContentType, common.XMLContentType)
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(client.DoWithParseFunc(req, parseFunc))
}

func (client *RestClient) DoXmlWithParseRes(
	method string,
	resource string,
	queryArgs url.Values,
	headers map[string]string,
	bodyModel interface{},
	parseFunc func(res *http.Response) error,
) error {
	bodyXml, err := xml.Marshal(bodyModel)
	if err != nil {
		return errors.WithStack(err)
	}

	req, err := client.NewRequestWithUrlQuery(method, resource, bytes.NewReader(bodyXml), queryArgs)
	req.Header.Set(common.HttpHeaderContentType, common.XMLContentType)
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(client.DoWithParseRes(req, parseFunc))
}

func (client *RestClient) DoXmlWithModel(
	method string,
	resource string,
	queryArgs url.Values,
	bodyModel interface{},
	resModel interface{},
) error {
	parseFunc := func(res *http.Response) error {
		decoder := xml.NewDecoder(res.Body)

		if resModel == nil {
			return nil
		}

		return errors.WithStack(decoder.Decode(resModel))
	}

	err := client.DoXmlWithParseFunc(method, resource, queryArgs, nil, bodyModel, parseFunc)
	return errors.WithStack(err)
}
