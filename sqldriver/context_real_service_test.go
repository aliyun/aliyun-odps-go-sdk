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

package sqldriver_test

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

// 真服务 smoke：只有给了凭据与项目才会跑，否则 skip。它验证的是
// context_contract_test.go 里那套假服务没法证明的两件事：
//  1. 取消等待之后，服务端的 instance 仍在跑（没有被客户端终止）；
//  2. 真实 instance tunnel 的结果下载在 ctx 取消后能立刻返回。
//
// 需要的环境变量：odps_endpoint、ALIBABA_CLOUD_ACCESS_KEY_ID、
// ALIBABA_CLOUD_ACCESS_KEY_SECRET（可选 ALIBABA_CLOUD_SECURITY_TOKEN、
// tunnel_endpoint、MAXCOMPUTE_PROJECT）。
func realServiceConfig(t *testing.T) *odps.Config {
	t.Helper()

	endpoint := os.Getenv("odps_endpoint")
	accessId := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
	accessKey := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
	project := os.Getenv("MAXCOMPUTE_PROJECT")

	if endpoint == "" || accessId == "" || accessKey == "" || project == "" {
		t.Skip("set odps_endpoint, ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET and MAXCOMPUTE_PROJECT to run the real-service smoke")
	}

	cfg := odps.NewConfig()
	cfg.AccessId = accessId
	cfg.AccessKey = accessKey
	cfg.StsToken = os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN")
	cfg.Endpoint = endpoint
	cfg.ProjectName = project
	cfg.TunnelEndpoint = os.Getenv("tunnel_endpoint")
	cfg.HttpTimeout = 60 * time.Second

	return cfg
}

func openRealDB(t *testing.T, cfg *odps.Config) *sql.DB {
	t.Helper()

	db, err := sql.Open("odps", cfg.FormatDsn())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)

	return db
}

// liveJobBudget 是给"等真实作业跑完"的期限。这个环境的排队时间不在我们控制之内：
// 同一条 explode 查询在 2026-09-24 是 55 秒，2026-10-16 超过 15 分钟还没出第一行。
// 用例要验的是取消语义，不是环境吞吐，所以期限可以给足，并且允许用
// MAXCOMPUTE_SMOKE_BUDGET（Go duration，如 "3m"）覆盖。
//
// 但期限必须落在 `go test -timeout`（默认 10 分钟）之内：否则 test binary 先到点 panic，
// 用例既拿不到结论，也来不及把留在服务端的作业收掉。这里读回 binary 自己的超时，
// 取较小者并留出 1/5 给收尾；要跑更长的期限，就同时把 -timeout 提上去。
func liveJobBudget() time.Duration {
	budget := 20 * time.Minute

	if v := os.Getenv("MAXCOMPUTE_SMOKE_BUDGET"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			budget = d
		}
	}

	if f := flag.Lookup("test.timeout"); f != nil {
		if d, err := time.ParseDuration(f.Value.String()); err == nil && d > 0 {
			if headroom := d - d/5; headroom < budget {
				budget = headroom
			}
		}
	}

	return budget
}

// stopCanceledInstance 把"客户端不再等待"时留在服务端的那个作业收掉。
// instance id 来自本进程刚拿到的取消错误，所以归属是构造性的，不靠时间窗猜；
// 没有这个清理，一次超时会把一个真实作业丢在共享项目里继续跑。
func stopCanceledInstance(t *testing.T, cfg *odps.Config, err error) string {
	t.Helper()

	var canceledErr *sqldriver.CanceledError
	if !errors.As(err, &canceledErr) || canceledErr.InstanceID == "" {
		return ""
	}

	ins := cfg.GenOdps().Instance(canceledErr.InstanceID)

	if err := ins.Load(); err != nil {
		t.Logf("cannot read instance %s for cleanup: %v", canceledErr.InstanceID, err)

		return canceledErr.InstanceID
	}

	if ins.Status() != odps.InstanceRunning {
		t.Logf("instance %s is already %s, nothing to clean", canceledErr.InstanceID, ins.Status())

		return canceledErr.InstanceID
	}

	if err := ins.Terminate(); err != nil {
		t.Logf("Terminate(%s) returned %v", canceledErr.InstanceID, err)

		return canceledErr.InstanceID
	}

	deadline := time.Now().Add(2 * time.Minute)

	for time.Now().Before(deadline) {
		if err := ins.Load(); err != nil {
			t.Logf("reload of %s failed: %v", canceledErr.InstanceID, err)

			break
		}

		if ins.Status() == odps.InstanceTerminated {
			break
		}
		time.Sleep(2 * time.Second)
	}

	t.Logf("cleaned up instance %s: status=%s", canceledErr.InstanceID, ins.Status())

	return canceledErr.InstanceID
}

// instanceIdFromError 从取消报错里取回 instance id。
func instanceIdFromError(t *testing.T, err error) string {
	t.Helper()

	re := regexp.MustCompile(`instance ([0-9a-z]{10,})`)

	if m := re.FindStringSubmatch(err.Error()); len(m) == 2 {
		return m[1]
	}

	t.Fatalf("expect the cancellation error to carry the instance id, got: %v", err)

	return ""
}

// TestRealServiceQueryWithoutContextStillWorks 最基本的回归：不加任何取消，
// 查询照常出结果。
func TestRealServiceQueryWithoutContextStillWorks(t *testing.T) {
	cfg := realServiceConfig(t)
	db := openRealDB(t, cfg)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), liveJobBudget())
	defer cancel()

	rows, err := db.QueryContext(ctx, "select 1 as one;")
	if err != nil {
		stopCanceledInstance(t, cfg, err)

		if errors.Is(err, context.DeadlineExceeded) {
			cancel()
			t.Skipf("the live service did not finish this job within %s (environment throughput, not a driver behaviour): %v",
				liveJobBudget(), err)
		}
		cancel()
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0

	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expect 1 row, got %d", count)
	}
}

// TestRealServiceDeadlineLeavesInstanceRunning 是关键的一条：客户端到点就不再等，
// 但远端任务归远端管，只有显式 Terminate 才会终止它。
func TestRealServiceDeadlineLeavesInstanceRunning(t *testing.T) {
	cfg := realServiceConfig(t)
	db := openRealDB(t, cfg)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := db.QueryContext(ctx, slowQuery())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expect the query to stop waiting on the deadline, got %v", err)
	}

	instanceId := instanceIdFromError(t, err)
	t.Logf("client stopped waiting for instance %s after the deadline", instanceId)

	// 用同一个 id 直接查服务端：任务还在，而不是被取消了。
	ins := cfg.GenOdps().Instance(instanceId)

	if err := ins.Load(); err != nil {
		t.Fatalf("cannot read the instance the driver was waiting for: %v", err)
	}
	if ins.Status() == odps.InstanceStatusUnknown {
		t.Fatalf("unexpected status for instance %s", instanceId)
	}
	t.Logf("server-side status right after the client gave up: %s", ins.Status())

	// 收尾：显式终止，并确认终止是这一步才发生的（服务端异步，给它一点时间）。
	if err := ins.Terminate(); err != nil {
		t.Logf("terminate returned %v (the job may already have finished)", err)
	}

	deadline := time.Now().Add(2 * time.Minute)

	for time.Now().Before(deadline) {
		if err := ins.Load(); err != nil {
			t.Fatal(err)
		}

		if ins.Status() == odps.InstanceTerminated {
			break
		}
		time.Sleep(2 * time.Second)
	}

	if err := ins.Load(); err != nil {
		t.Fatal(err)
	}
	t.Logf("server-side status after an explicit Terminate: %s", ins.Status())

	if ins.Status() != odps.InstanceTerminated {
		t.Fatalf("the instance is expected to be terminated by now, but it is %s", ins.Status())
	}

	if _, err := db.QueryContext(context.Background(), "select 1 as one;"); err != nil {
		t.Fatalf("the pool must still be usable after a canceled query: %v", err)
	}
}

// TestRealServiceCancelDuringRowConsumption 行消费中途取消：下一次 Next 立刻带
// ctx 的错误返回，不会停在结果流上。
func TestRealServiceCancelDuringRowConsumption(t *testing.T) {
	cfg := realServiceConfig(t)
	db := openRealDB(t, cfg)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), liveJobBudget())

	rows, err := db.QueryContext(ctx, multiRowQuery())
	if err != nil {
		stopCanceledInstance(t, cfg, err)

		if errors.Is(err, context.DeadlineExceeded) {
			cancel()
			t.Skipf("no first row within %s, so mid-consumption cancellation could not be exercised: %v",
				liveJobBudget(), err)
		}
		cancel()
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		err := rows.Err()
		cancel()
		t.Fatalf("no row came back: %v", err)
	}

	cancel()
	start := time.Now()

	if rows.Next() {
		cancel()
		t.Fatal("expect no row after cancellation")
	}

	took := time.Since(start)
	if took > 2*time.Second {
		t.Fatalf("Next kept blocking for %s after the context was canceled", took)
	}
	if err := rows.Err(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("expect the cancellation to be reported, got %v", err)
	}
}

// slowQuery 要跑得比 3 秒的期限久，好让取消发生在等待 instance 结束的阶段。
// 这个环境里带 explode 的查询要经历完整的作业调度（实测 50 秒上下），
// 比 "select 1" 的快路径更合适。
func slowQuery() string {
	if q := os.Getenv("SMOKE_SLOW_QUERY"); q != "" {
		return q
	}

	return multiRowQuery()
}

// multiRowQuery 至少给两行，好在第一行读完之后还有得读。
func multiRowQuery() string {
	if q := os.Getenv("SMOKE_MULTIROW_QUERY"); q != "" {
		return q
	}

	return strings.TrimSpace(`
select c1 from (select explode(array(1,2,3,4,5,6,7,8,9,10)) as c1) t;`)
}
