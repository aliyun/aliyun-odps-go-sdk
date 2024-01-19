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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/pkg/errors"
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
const (
	DefaultHttpTimeout          = 5
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
	_client              *http.Client
	defaultProject       string
	endpoint             string
	userAgent            string
}

func NewOdpsRestClient(a account.Account, endpoint string) RestClient {
	var client = RestClient{
		Account:              a,
		endpoint:             endpoint,
		HttpTimeout:          DefaultHttpTimeout * time.Second,
		TcpConnectionTimeout: DefaultTcpConnectionTimeout * time.Second,
		DnsCacheExpireTime:   time.Duration(DefaultDNSCacheExpireTime) * time.Second,
		DisableCompression:   true,
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

func (client *RestClient) SetUserAgent(userAgent string) {
	client.userAgent = userAgent
}

func (client *RestClient) UserAgent() string {
	if client.userAgent != "" {
		return client.userAgent
	}

	return common.UserAgentValue
}

func (client *RestClient) Endpoint() string {
	return client.endpoint
}

func (client *RestClient) client() *http.Client {
	if client._client != nil {
		return client._client
	}

	resolver := NewResolver(int64(client.DnsCacheExpireTime) / int64(time.Second))

	dialer := Dialer{
		Resolver: resolver,
		Dialer: net.Dialer{
			Timeout:   client.TcpConnectionTimeout,
			KeepAlive: 30 * time.Second,
		},
	}

	var transport = http.Transport{
		Proxy:              http.ProxyFromEnvironment,
		DialContext:        dialer.DialContext,
		ForceAttemptHTTP2:  false,
		DisableKeepAlives:  true,
		DisableCompression: client.DisableCompression,
	}

	client._client = &http.Client{
		Transport: &transport,
		Timeout:   client.HttpTimeout,
	}

	return client._client
}

func (client *RestClient) NewRequest(method, resource string, body io.Reader) (*http.Request, error) {
	var urlStr = fmt.Sprintf(
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

	client.SignRequest(req, client.endpoint)

	res, err := client.client().Do(req)
	return res, errors.WithStack(err)
}

func (client *RestClient) DoWithParseFunc(req *http.Request, parseFunc func(res *http.Response) error) error {
	return client.DoWithParseRes(req, func(res *http.Response) error {
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return errors.WithStack(NewHttpNotOk(res))
		}

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

func (client *RestClient) GetWithModel(resource string, queryArgs url.Values, model interface{}) error {
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(client.DoWithModel(req, model))
}

func (client *RestClient) GetWithParseFunc(resource string, queryArgs url.Values, parseFunc func(res *http.Response) error) error {
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
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
	parseFunc func(res *http.Response) error) error {

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
	parseFunc func(res *http.Response) error) error {

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
	resModel interface{}) error {

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
