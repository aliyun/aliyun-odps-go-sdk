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

package odps

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

// WaitForSuccessContext 在 ctx 已取消时立即返回，不发请求。
func TestWaitForSuccessContextAlreadyCanceled(t *testing.T) {
	instance := NewInstance(nil, "some_project", "some_instance")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := instance.WaitForSuccessContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expect context.Canceled, but got %v", err)
	}
}

// WaitForSuccessContext 在实例还在跑的时候按 ctx 的期限返回，并且返回的
// 错误链里保留 context 的错误；等待期间不终止服务端实例。
func TestWaitForSuccessContextStopsOnDeadline(t *testing.T) {
	var statusGets, terminatePuts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/instances/some_instance"):
			atomic.AddInt32(&statusGets, 1)
			w.Header().Set("Content-Type", "application/xml")
			if _, ok := r.URL.Query()["taskstatus"]; ok {
				_, _ = w.Write([]byte(`<Instance><Tasks><Task Type="SQL"><Name>t1</Name><Status>Running</Status></Task></Tasks></Instance>`))
			} else {
				_, _ = w.Write([]byte(`<Instance><Status>Running</Status></Instance>`))
			}
		case r.Method == http.MethodPut:
			atomic.AddInt32(&terminatePuts, 1)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected "+r.URL.String(), http.StatusNotFound)
		}
	}))
	defer server.Close()

	odpsIns := NewOdps(account2.NewAliyunAccount("fake-access-id", "fake-access-key"), server.URL)
	odpsIns.SetDefaultProjectName("some_project")
	instance := NewInstance(odpsIns, "some_project", "some_instance")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := instance.WaitForSuccessContext(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expect context.DeadlineExceeded, but got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("the deadline was not honored, waited %s", elapsed)
	}
	if got := atomic.LoadInt32(&statusGets); got == 0 {
		t.Fatal("the instance status was never polled")
	}
	if got := atomic.LoadInt32(&terminatePuts); got != 0 {
		t.Fatalf("waiting must not terminate the instance, but %d PUT request(s) were sent", got)
	}
}

// 未取消的 context 下行为与 WaitForSuccess 一致：轮询到任务成功后返回 nil。
func TestWaitForSuccessContextSucceeds(t *testing.T) {
	var polls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")

		if _, ok := r.URL.Query()["taskstatus"]; ok {
			n := atomic.AddInt32(&polls, 1)
			status := "Running"
			if n >= 2 {
				status = "Success"
			}
			_, _ = w.Write([]byte(`<Instance><Tasks><Task Type="SQL"><Name>t1</Name><Status>` + status + `</Status></Task></Tasks></Instance>`))

			return
		}

		_, _ = w.Write([]byte(`<Instance><Status>Running</Status></Instance>`))
	}))
	defer server.Close()

	odpsIns := NewOdps(account2.NewAliyunAccount("fake-access-id", "fake-access-key"), server.URL)
	odpsIns.SetDefaultProjectName("some_project")
	instance := NewInstance(odpsIns, "some_project", "some_instance")

	if err := instance.WaitForSuccessContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&polls); got < 2 {
		t.Fatalf("expect at least two polls, but got %d", got)
	}
}
