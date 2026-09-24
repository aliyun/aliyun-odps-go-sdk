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
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

// RestClient 在 SDK 里被大量按值拷贝（client := odpsIns.restClient）。
// 这两条用例钉住拷贝语义修复后的两个要求：连接池在同一个 RestClient 及其拷贝之间
// 共用；但改了超时之后再请求必须生效，不能悄悄留着第一次建好的 client。

// newCountingServer 返回一个假服务与它的连接计数。ConnState 必须在 Start() 之前
// 装好：httptest.Server 已经跑起来之后再写 Config.ConnState 会和 server 自己的
// 读取竞争（-race 会报）。
func newCountingServer(t *testing.T, delay time.Duration) (url string, opened, closed *int32) {
	t.Helper()

	var openedValue, closedValue int32

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			atomic.AddInt32(&openedValue, 1)
		case http.StateClosed:
			atomic.AddInt32(&closedValue, 1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	return srv.URL, &openedValue, &closedValue
}

// requestThroughCopy 用一次值拷贝发请求，模拟 SDK 里 client := odpsIns.restClient
// 这种调用点写法。
func requestThroughCopy(client RestClient) error {
	copied := client

	req, err := copied.NewRequestWithUrlQuery("GET", "/projects/p", nil, url.Values{"x": []string{"1"}})
	if err != nil {
		return err
	}

	res, err := copied.Do(req)
	if err != nil {
		return err
	}

	_, _ = ioutil.ReadAll(res.Body)

	return res.Body.Close()
}

func TestCopiedRestClientsShareOneConnectionPool(t *testing.T) {
	endpoint, opened, _ := newCountingServer(t, 0)

	client := NewOdpsRestClient(account.NewAliyunAccount("id", "key"), endpoint)

	for i := 0; i < 5; i++ {
		if err := requestThroughCopy(client); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}

	if got := atomic.LoadInt32(opened); got != 1 {
		t.Fatalf("5 requests through copies must reuse one connection, but %d were opened", got)
	}
}

func TestChangedTimeoutIsAppliedToNextRequest(t *testing.T) {
	// 服务端每个请求 300ms，方便看出超时到底生效没有。
	endpoint, _, _ := newCountingServer(t, 300*time.Millisecond)

	client := NewOdpsRestClient(account.NewAliyunAccount("id", "key"), endpoint)
	client.HttpTimeout = 5 * time.Second

	// 先用宽松超时把连接池建起来。
	if err := requestThroughCopy(client); err != nil {
		t.Fatalf("the loose-timeout request should succeed: %v", err)
	}

	// 收紧超时之后的拷贝必须拿得到新超时。连接池是共享的，如果只认第一次建好的
	// client，这里就会一直宽松下去，静默改掉调用方的意图。
	tight := client
	tight.HttpTimeout = 50 * time.Millisecond

	if err := requestThroughCopy(tight); err == nil {
		t.Fatal("expect the tightened HttpTimeout to take effect, but the request succeeded")
	}

	// 换回原来的配置仍然可用（共享状态没有被改坏）。
	back := client
	back.HttpTimeout = 5 * time.Second

	if err := requestThroughCopy(back); err != nil {
		t.Fatalf("the loose-timeout request should succeed again: %v", err)
	}
}
