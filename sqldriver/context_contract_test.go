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

package sqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

// 本文件是 database/sql Context 取消与关闭契约的离线回归：一个假的 MaxCompute
// 服务（REST + instance tunnel）驱动真实的 sqldriver 代码路径，不需要云凭据。
// 真服务上的 smoke 结果记录在工作项里，不在这里重复。

const (
	fakeProjectName = "go_sdk_ctx_contract"
	fakeInstanceId  = "20260924000000000fake1"
	fakeTaskName    = "console_query_task_fake"
	neverSucceeds   = 1 << 20
)

const (
	streamComplete = iota
	streamStallAfterFirstRecord
)

// fakeService 扮演 MaxCompute REST 端点与 instance tunnel 端点，
// 只实现 sqldriver.queryContext 真正会打到的那几个请求。
type fakeService struct {
	rest   *httptest.Server
	tunnel *httptest.Server

	mu sync.Mutex

	submits             int
	instanceStatusGets  int
	taskStatusGets      int
	sessionCreates      int
	dataRequests        int
	terminates          int
	succeedAfterPolls   int
	streamMode          int
	abandonedStreamRead int32
}

func newFakeService(t *testing.T) *fakeService {
	t.Helper()

	f := &fakeService{succeedAfterPolls: neverSucceeds}
	f.rest = httptest.NewServer(http.HandlerFunc(f.restHandler))
	f.tunnel = httptest.NewServer(http.HandlerFunc(f.tunnelHandler))
	t.Cleanup(func() {
		f.rest.Close()
		f.tunnel.Close()
	})

	return f
}

func (f *fakeService) config() *odps.Config {
	cfg := odps.NewConfig()
	cfg.AccessId = "fake-access-id"
	cfg.AccessKey = "fake-access-key"
	cfg.ProjectName = fakeProjectName
	cfg.Endpoint = f.rest.URL
	// 直接给定 tunnel 端点，跳过 GetTunnelEndpoint 寻址：生产 DSN 也常这么配。
	cfg.TunnelEndpoint = f.tunnel.URL
	cfg.HttpTimeout = 15 * time.Second

	return cfg
}

func (f *fakeService) dsn() string {
	return f.config().FormatDsn()
}

func (f *fakeService) newConnection() *connection {
	return newConnection(f.config())
}

func (f *fakeService) count(field *int) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return *field
}

func (f *fakeService) restHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	query := r.URL.Query()

	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/instances"):
		f.mu.Lock()
		f.submits++
		f.mu.Unlock()
		w.Header().Set("Location", fmt.Sprintf("%s/%s", path, fakeInstanceId))
		w.WriteHeader(http.StatusCreated)

		return

	case r.Method == http.MethodPut && strings.Contains(path, "/instances/"):
		f.mu.Lock()
		f.terminates++
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)

		return

	case r.Method == http.MethodGet && strings.Contains(path, "/instances/"):
		if _, ok := query["taskstatus"]; ok {
			f.mu.Lock()
			f.taskStatusGets++
			polls := f.taskStatusGets
			done := polls >= f.succeedAfterPolls
			f.mu.Unlock()

			status := "Running"
			if done {
				status = "Success"
			}
			writeXml(w, fmt.Sprintf(
				`<Instance><Tasks><Task Type="SQL"><Name>%s</Name><Status>%s</Status></Task></Tasks></Instance>`,
				fakeTaskName, status))

			return
		}

		f.mu.Lock()
		f.instanceStatusGets++
		f.mu.Unlock()
		writeXml(w, `<Instance><Status>Running</Status></Instance>`)

		return
	}

	http.Error(w, "unexpected request "+r.Method+" "+r.URL.String(), http.StatusNotFound)
}

func (f *fakeService) tunnelHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	if r.Method == http.MethodPost {
		f.mu.Lock()
		f.sessionCreates++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"DownloadID": "20260924000000fake",
			"RecordCount": 2,
			"Status": "NORMAL",
			"Schema": {"columns": [{"name": "c_string", "type": "string", "nullable": true}], "partitionKeys": []}
		}`))

		return
	}

	if _, ok := query["data"]; ok {
		f.mu.Lock()
		f.dataRequests++
		mode := f.streamMode
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		columns := []tableschema.Column{{Name: "c_string", Type: datatype.StringType}}
		flusher, _ := w.(http.Flusher)

		if mode == streamStallAfterFirstRecord {
			// 先给出第一条完整记录，然后挂住：Close 必须能解除这个阻塞读。
			_, _ = w.Write(encodeTunnelRecords(columns, []string{"row-0"}, false))
			if flusher != nil {
				flusher.Flush()
			}
			<-r.Context().Done()
			atomic.StoreInt32(&f.abandonedStreamRead, 1)

			return
		}

		_, _ = w.Write(encodeTunnelRecords(columns, []string{"row-0", "row-1"}, true))
		if flusher != nil {
			flusher.Flush()
		}

		return
	}

	http.Error(w, "unexpected tunnel request "+r.Method+" "+r.URL.String(), http.StatusNotFound)
}

func writeXml(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + body))
}

// encodeTunnelRecords 按 instance tunnel 的记录格式生成结果流，与
// odps/tunnel.RecordProtocReader 期望的字节序和 CRC 一致：每个字段先写 tag，
// CRC32C 依次喂入列号（int32 小端）与字段值字节；记录尾是 EndRecord tag 加记录 CRC；
// 流尾是 MetaCount 与 MetaChecksum，且之后必须正好是流的结束。
func encodeTunnelRecords(columns []tableschema.Column, values []string, withMeta bool) []byte {
	table := crc32.MakeTable(crc32.Castagnoli)
	var buf []byte
	crcOfCrc := uint32(0)

	for _, value := range values {
		recordCrc := uint32(0)
		for i := range columns {
			colIndex := int32(i + 1)
			buf = protowire.AppendTag(buf, protowire.Number(colIndex), protowire.BytesType)
			b := []byte(value)
			buf = protowire.AppendBytes(buf, b)
			recordCrc = crc32.Update(recordCrc, table, littleEndian32(uint32(colIndex)))
			recordCrc = crc32.Update(recordCrc, table, b)
		}
		buf = protowire.AppendTag(buf, tunnel.EndRecord, protowire.VarintType)
		buf = protowire.AppendVarint(buf, uint64(recordCrc))
		crcOfCrc = crc32.Update(crcOfCrc, table, littleEndian32(recordCrc))
	}

	if !withMeta {
		return buf
	}

	buf = protowire.AppendTag(buf, tunnel.MetaCount, protowire.VarintType)
	buf = protowire.AppendVarint(buf, protowire.EncodeZigZag(int64(len(values))))
	buf = protowire.AppendTag(buf, tunnel.MetaChecksum, protowire.VarintType)
	buf = protowire.AppendVarint(buf, uint64(crcOfCrc))

	return buf
}

func littleEndian32(v uint32) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}

// callWithin 在限期内跑 f，返回耗时与错误，避免测试自己挂死。
func callWithin(t *testing.T, limit time.Duration, f func() error) (time.Duration, error) {
	t.Helper()

	done := make(chan error, 1)
	start := time.Now()

	go func() { done <- f() }()

	select {
	case err := <-done:
		return time.Since(start), err
	case <-time.After(limit):
		t.Fatalf("the call did not return within %s", limit)
		return 0, nil
	}
}

// --- 取消传播 ---

func TestQueryContextAlreadyCanceledSubmitsNothing(t *testing.T) {
	f := newFakeService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.newConnection().QueryContext(ctx, "select 'x';", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expect context.Canceled, but got %v", err)
	}
	if got := f.count(&f.submits); got != 0 {
		t.Fatalf("a canceled context must not create a MaxCompute instance, but %d submit request(s) arrived", got)
	}
}

func TestExecContextAlreadyCanceledSubmitsNothing(t *testing.T) {
	f := newFakeService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.newConnection().ExecContext(ctx, "drop table if exists not_a_real_table;", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expect context.Canceled, but got %v", err)
	}
	if got := f.count(&f.submits); got != 0 {
		t.Fatalf("a canceled context must not create a MaxCompute instance, but %d submit request(s) arrived", got)
	}
}

// TestQueryContextDeadlineStopsWaiting 是本项复现的核心：服务端任务还在跑，
// 调用方给的期限已经到了。修复前 QueryContext 完全忽略 ctx，会一直轮询到任务
// 结束（这里被配成永远不成功，所以修复前只会在 limit 上失败）。
func TestQueryContextDeadlineStopsWaiting(t *testing.T) {
	f := newFakeService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	elapsed, err := callWithin(t, 5*time.Second, func() error {
		_, err := f.newConnection().QueryContext(ctx, "select 'x';", nil)
		return err
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expect context.DeadlineExceeded, but got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("waiting ignored the deadline: it returned only after %s", elapsed)
	}
	if !strings.Contains(err.Error(), fakeInstanceId) {
		t.Fatalf("the cancellation error should carry the instance id so the caller can track or terminate the job, got %v", err)
	}
	if got := f.count(&f.terminates); got != 0 {
		t.Fatalf("client-side cancellation must not terminate the remote instance, but %d PUT request(s) arrived", got)
	}
}

func TestQueryContextCancelStopsWaiting(t *testing.T) {
	f := newFakeService(t)
	ctx, cancel := context.WithCancel(context.Background())

	returned := make(chan error, 1)

	go func() {
		_, err := f.newConnection().QueryContext(ctx, "select 'x';", nil)
		returned <- err
	}()

	// 等到第一次状态轮询，确保是“等待中取消”而不是一开始就取消。
	if err := waitFor(3*time.Second, func() bool { return f.count(&f.taskStatusGets) >= 1 }); err != nil {
		t.Fatalf("the driver never polled the instance status: %v", err)
	}

	cancelAt := time.Now()
	cancel()

	var err error

	select {
	case err = <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("QueryContext did not return after the context was canceled")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expect context.Canceled, but got %v", err)
	}
	if took := time.Since(cancelAt); took > 3*time.Second {
		t.Fatalf("cancellation was noticed only after %s, expected within one poll interval", took)
	}
}

func TestExecContextDeadlineStopsWaiting(t *testing.T) {
	f := newFakeService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	elapsed, err := callWithin(t, 5*time.Second, func() error {
		_, err := f.newConnection().ExecContext(ctx, "create table if not exists t(a string);", nil)
		return err
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expect context.DeadlineExceeded, but got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("waiting ignored the deadline: it returned only after %s", elapsed)
	}
}

// --- Ping 的实际语义 ---

// TestPingContextSemantics 固定一个容易误解的事实：本 driver 没有实现
// driver.Pinger，所以 db.PingContext 不会访问服务端。它只在 database/sql 层拿
// 连接：ctx 已取消时返回 ctx.Err()，否则一律返回 nil —— 包括端点不可用的情况。
// 想真正探活必须显式跑一条 SQL。
func TestPingContextSemantics(t *testing.T) {
	f := newFakeService(t)

	db, err := sql.Open("odps", f.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := db.PingContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expect PingContext to honor a canceled context, but got %v", err)
	}
	if _, isPinger := interface{}(f.newConnection()).(driver.Pinger); isPinger {
		t.Fatal("the expectations below assume the driver does not implement driver.Pinger")
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := f.count(&f.submits) + f.count(&f.instanceStatusGets); got != 0 {
		t.Fatalf("Ping should not touch the service, but %d request(s) arrived", got)
	}
}

// --- 结果消费与关闭 ---

func TestQueryContextCancelDuringRowConsumption(t *testing.T) {
	f := newFakeService(t)
	f.succeedAfterPolls = 1
	f.streamMode = streamStallAfterFirstRecord

	db, err := sql.Open("odps", f.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

	rows, err := db.QueryContext(ctx, "select c_string from t;")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer rows.Close()

	var first string
	if !rows.Next() {
		rows.Close()
		cancel()
		t.Fatalf("expect the first row to be readable, err=%v", rows.Err())
	}
	if err := rows.Scan(&first); err != nil {
		rows.Close()
		cancel()
		t.Fatal(err)
	}

	// 行消费中途取消：第二条记录永远不会到，Next 只能靠 Close 解除阻塞。
	cancel()
	start := time.Now()

	if rows.Next() {
		rows.Close()
		cancel()
		t.Fatal("expect no more rows after cancellation")
	}

	took := time.Since(start)
	err = rows.Err()
	rows.Close()
	cancel()

	if took > 3*time.Second {
		t.Fatalf("row consumption did not stop after cancellation, took %s", took)
	}
	if err := waitFor(3*time.Second, func() bool {
		return atomic.LoadInt32(&f.abandonedStreamRead) == 1
	}); err != nil {
		t.Fatalf("the result stream was never released: %v", err)
	}
	// 截断的结果集必须报错，不能看起来像正常读完了。
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expect rows.Err() to report the cancellation, but got %v", err)
	}
}

func TestRowsCloseIsIdempotentAndStopsReading(t *testing.T) {
	inner := &fakeRecordReader{records: []data.Record{stringRecord("a")}}
	rows := &rowsReader{
		columns: []tableschema.Column{{Name: "c_string", Type: datatype.StringType}},
		inner:   inner,
	}

	dst := make([]driver.Value, 1)
	if err := rows.Next(dst); err != nil {
		t.Fatal(err)
	}
	if dst[0] != "a" {
		t.Fatalf("expect a, but got %v", dst[0])
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, but got %v", err)
	}
	if got := atomic.LoadInt32(&inner.closes); got != 1 {
		t.Fatalf("the underlying stream must be closed exactly once, but closes=%d", got)
	}
	if err := rows.Next(dst); !errors.Is(err, io.EOF) {
		t.Fatalf("expect io.EOF after Close, but got %v", err)
	}
	if got := atomic.LoadInt32(&inner.reads); got != 1 {
		t.Fatalf("no read is expected after Close, but reads=%d", got)
	}
}

func TestRowsCloseWithNilInnerAndNextAfterClose(t *testing.T) {
	rows := &rowsReader{columns: []tableschema.Column{{Name: "c_string", Type: datatype.StringType}}}

	if err := rows.Close(); err != nil {
		t.Fatalf("Close on a rowsReader without a stream must be a no-op, but got %v", err)
	}
	if err := rows.Next(make([]driver.Value, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("expect io.EOF, but got %v", err)
	}
}

// TestRowsCloseConcurrentWithNext 覆盖 database/sql 在 ctx 取消时的真实形态：
// Close 来自另一个 goroutine，与进行中的 Read 并发。加 -race 跑才有意义。
func TestRowsCloseConcurrentWithNext(t *testing.T) {
	inner := &fakeRecordReader{
		records:           []data.Record{stringRecord("a"), stringRecord("b")},
		blockOnSecondRead: true,
		blocked:           make(chan struct{}),
	}
	rows := &rowsReader{
		columns: []tableschema.Column{{Name: "c_string", Type: datatype.StringType}},
		inner:   inner,
	}

	dst := make([]driver.Value, 1)
	// 第一行正常读掉，第二次读才会停在“网络挂住”上。
	if err := rows.Next(dst); err != nil {
		t.Fatal(err)
	}

	nextDone := make(chan error, 1)

	go func() {
		nextDone <- rows.Next(dst)
	}()

	select {
	case <-inner.blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("the fake reader never blocked")
	}

	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	var err error

	select {
	case err = <-nextDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock the pending read")
	}

	if err == nil {
		t.Fatal("expect the blocked read to report an error")
	}
	if err := rows.Next(dst); !errors.Is(err, io.EOF) {
		t.Fatalf("expect io.EOF after Close, but got %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, but got %v", err)
	}
}

// --- 正常路径、连接池再用与 goroutine 泄漏 ---

func TestQueryReadsAllRowsFromFakeTunnel(t *testing.T) {
	f := newFakeService(t)
	f.succeedAfterPolls = 1

	db, err := sql.Open("odps", f.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "select c_string from t;")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []string

	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "row-0" || got[1] != "row-1" {
		t.Fatalf("expect [row-0 row-1], but got %v", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, but got %v", err)
	}
}

// TestConnectionReusableAfterCanceledQuery 取消一次查询后，同一条连接必须还能
// 跑完下一个查询：连接没有被取消状态污染，结果也没有串台。
func TestConnectionReusableAfterCanceledQuery(t *testing.T) {
	f := newFakeService(t)
	f.succeedAfterPolls = 2

	db, err := sql.Open("odps", f.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	tight, cancelTight := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err = db.QueryContext(tight, "select 'slow';")
	cancelTight()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expect the first query to time out, but got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "select c_string from t;")
	if err != nil {
		t.Fatalf("the pooled connection is unusable after a canceled query: %v", err)
	}

	count := 0

	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	if count != 2 {
		t.Fatalf("expect 2 rows from the follow-up query, but got %d", count)
	}
	if got := f.count(&f.submits); got != 2 {
		t.Fatalf("expect 2 submitted instances, but got %d", got)
	}
}

// 关于 goroutine 泄漏的判据：一次查询可能留下一两条 keep-alive 连接的
// readLoop/writeLoop（transport 的 idle 连接），那是常量级的开销，不是泄漏。
// 真正的泄漏会随查询次数线性增长，所以这里跑两批同样的操作，要求第二批
// 带来的增量不再增长。
const goroutinePlateauSlack = 6

func runCanceledQueries(t *testing.T, db *sql.DB, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := db.QueryContext(ctx, "select 'x';")
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}

// TestNoGoroutineLeakAfterCanceledQueries 反复取消查询不应留下等待用的
// goroutine：取消发生在状态轮询之间，不额外起 goroutine。
func TestNoGoroutineLeakAfterCanceledQueries(t *testing.T) {
	f := newFakeService(t)

	db, err := sql.Open("odps", f.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)

	runCanceledQueries(t, db, 30)
	afterFirst := stableGoroutineCount()

	runCanceledQueries(t, db, 30)

	if got := stableGoroutineCount(); got > afterFirst+goroutinePlateauSlack {
		t.Fatalf("goroutines keep growing with the number of canceled queries: %d after the first batch, %d after the second;%s",
			afterFirst, got, goroutineSummary())
	}
}

// TestNoGoroutineLeakForNormalQueries 正常查询也会为可取消的 ctx 起一个取消
// 监听 goroutine，它必须在 Close 时退出：否则每查一次就漏一个。
func TestNoGoroutineLeakForNormalQueries(t *testing.T) {
	f := newFakeService(t)
	f.succeedAfterPolls = 1

	db, err := sql.Open("odps", f.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)

	runSuccessfulQueries(t, db, 20)
	afterFirst := stableGoroutineCount()

	runSuccessfulQueries(t, db, 20)

	if got := stableGoroutineCount(); got > afterFirst+goroutinePlateauSlack {
		t.Fatalf("goroutines keep growing with the number of queries: %d after the first batch, %d after the second;%s",
			afterFirst, got, goroutineSummary())
	}
}

func runSuccessfulQueries(t *testing.T, db *sql.DB, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		rows, err := db.QueryContext(ctx, "select c_string from t;")
		if err != nil {
			cancel()
			t.Fatalf("iteration %d: %v", i, err)
		}

		count := 0

		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if count != 2 {
			t.Fatalf("iteration %d: expect 2 rows, got %d", i, count)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}

// --- 取消错误的可用性与 Exec 的返回契约 ---

// TestCanceledErrorCarriesInstanceIdForCancel：调用方不用解析错误文本就能拿到
// instance id，同时 errors.Is 对 context 错误的判断保持不变。
func TestCanceledErrorCarriesInstanceIdForCancel(t *testing.T) {
	f := newFakeService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := f.newConnection().QueryContext(ctx, "select 'x';", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("errors.Is must still see the context error, got %v", err)
	}

	var canceledErr *CanceledError
	if !errors.As(err, &canceledErr) {
		t.Fatalf("expect a *sqldriver.CanceledError, got %T: %v", err, err)
	}
	if canceledErr.InstanceID != fakeInstanceId {
		t.Fatalf("expect instance id %s, got %q", fakeInstanceId, canceledErr.InstanceID)
	}
	if !errors.Is(canceledErr, context.DeadlineExceeded) {
		t.Fatal("CanceledError must unwrap to the context error")
	}
}

// TestExecContextReturnsUsableResult：ExecContext 成功时不能给 nil driver.Result。
// 修复前 database/sql 把 nil 原样交给调用方，res.RowsAffected() 直接 panic。
func TestExecContextReturnsUsableResult(t *testing.T) {
	f := newFakeService(t)
	f.succeedAfterPolls = 1

	db, err := sql.Open("odps", f.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := db.ExecContext(ctx, "create table if not exists t(a string);")
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("ExecContext must not hand back a nil sql.Result")
	}

	// 这两行在修复前会 panic（nil interface 上调方法）。
	if _, err := res.RowsAffected(); !errors.Is(err, ErrNoRowsAffectedInfo) {
		t.Fatalf("expect the documented ErrNoRowsAffectedInfo, got %v", err)
	}
	if _, err := res.LastInsertId(); !errors.Is(err, ErrNoLastInsertIdInfo) {
		t.Fatalf("expect the documented ErrNoLastInsertIdInfo, got %v", err)
	}
}

// --- helpers ---

type fakeRecordReader struct {
	records           []data.Record
	blockOnSecondRead bool

	pos     int32
	reads   int32
	closes  int32
	blocked chan struct{}
}

func stringRecord(v string) data.Record {
	return data.Record{data.String(v)}
}

func (r *fakeRecordReader) Read() (data.Record, error) {
	atomic.AddInt32(&r.reads, 1)
	pos := atomic.AddInt32(&r.pos, 1)

	if r.blockOnSecondRead && pos >= 2 {
		if r.blocked != nil {
			close(r.blocked)
		}
		// 模拟一次悬挂在网络读上的 Next：只有 Close 能让它结束。
		for atomic.LoadInt32(&r.closes) == 0 {
			time.Sleep(time.Millisecond)
		}

		return nil, io.ErrClosedPipe
	}

	if int(pos) > len(r.records) {
		return nil, io.EOF
	}

	return r.records[pos-1], nil
}

func (r *fakeRecordReader) Close() error {
	atomic.AddInt32(&r.closes, 1)

	return nil
}

// goroutineSummary 统计 goroutine 卡在哪个函数上（取栈里第一个“像业务”的帧，
// 并把地址与参数去掉，否则每条栈都是唯一 key）。失败时用它说明泄漏形态。
func goroutineSummary() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	counts := make(map[string]int)
	replacer := strings.NewReplacer()

	for _, record := range strings.Split(string(buf[:n]), "\n\n") {
		lines := strings.Split(record, "\n")
		frame := "unknown"

		for i, line := range lines[1:] {
			// 栈里函数行与文件行交替出现，只看函数行。
			if i%2 != 0 {
				continue
			}
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "created by") {
				continue
			}
			frame = shortenFrame(line)
			break
		}

		counts[frame]++
	}

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	_ = replacer

	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}

		return keys[i] < keys[j]
	})

	var b strings.Builder
	b.WriteString("\n")

	for i, k := range keys {
		if i >= 6 {
			break
		}
		fmt.Fprintf(&b, "%4d x %s\n", counts[k], k)
	}

	return b.String()
}

// shortenFrame 从 "path/pkg.Func(0x123, 0x4)" 这种栈行里取出 "pkg.Func"。
func shortenFrame(line string) string {
	if at := strings.Index(line, "("); at > 0 {
		line = line[:at]
	}
	if at := strings.LastIndex(line, "/"); at > 0 {
		line = line[at+1:]
	}

	return line
}

func waitFor(limit time.Duration, cond func() bool) error {
	deadline := time.Now().Add(limit)

	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}

	return fmt.Errorf("condition not met within %s", limit)
}

// stableGoroutineCount 等 goroutine 数稳定后再取，避免抖动导致假失败。
func stableGoroutineCount() int {
	previous := -1

	for i := 0; i < 40; i++ {
		runtime.GC()
		count := runtime.NumGoroutine()
		if count == previous {
			return count
		}
		previous = count
		time.Sleep(50 * time.Millisecond)
	}

	return runtime.NumGoroutine()
}
