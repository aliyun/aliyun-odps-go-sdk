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
	"database/sql/driver"
	"log"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

type connection struct {
	connector      *Connector
	closeConnector bool
}

// Begin reports that MaxCompute does not support database/sql transactions.
func (c *connection) Begin() (driver.Tx, error) {
	return nil, ErrTransactionsUnsupported
}

// Prepare reports that the driver does not support prepared statements.
func (c *connection) Prepare(string) (driver.Stmt, error) {
	return nil, ErrPreparedStatementsUnsupported
}

// Close releases connections owned by a direct Driver.Open call.
// Pooled connections share the connector's client until DB.Close.
func (c *connection) Close() error {
	if c.closeConnector {
		return c.connector.Close()
	}
	return nil
}

// QueryContext uses ctx for submission, waiting, and reading results. An
// interrupted operation with a known instance ID returns CanceledError.
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlStr, err := bindSQL(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.queryContext(ctx, sqlStr)
}

func (c *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	sqlStr, err := positionArgQueryToSql(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.queryContext(context.Background(), sqlStr)
}

func (c *connection) queryContext(ctx context.Context, query string) (driver.Rows, error) {
	// 提交前先检查一次：ctx 已经取消时不去创建远端 instance。
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	opts := c.connector.options
	job, err := c.submit(ctx, query)
	if err != nil {
		return nil, instanceError(job, err)
	}
	result, err := job.Wait(ctx)
	if err != nil {
		return nil, instanceError(job, err)
	}

	if opts.EnableLogView {
		link, err := job.LogView(ctx, 10)
		if err != nil {
			return nil, instanceError(job, errors.Wrap(err, "generate LogView"))
		}
		log.Printf("%s\n", link)
	}

	session, err := tunnel.OpenSQLResult(ctx, c.connector.client, result.Ref(), tunnel.ResultOptions{
		Endpoint: opts.TunnelEndpoint, QuotaName: opts.TunnelQuotaName, DisableKeepAlives: true,
	})
	if err != nil {
		return nil, instanceError(job, err)
	}

	recordCount := session.RecordCount()
	if recordCount == 0 {
		recordCount = 1
	}

	reader, err := session.OpenRecordReaderContext(ctx, 0, recordCount, 0, nil)
	if err != nil {
		session.RestClient.CloseIdleConnections()
		return nil, instanceError(job, err)
	}

	schema := session.Schema()
	rows := &rowsReader{
		columns: schema.Columns,
		inner:   reader,
		ctx:     ctx,
		// 注意取的是 session 里那个 RestClient 的地址：它的 http.Client 是
		// 惰性建的，值拷贝会拿到一个还没建过连接的副本，关不掉任何东西。
		releaseIdleConns: session.RestClient.CloseIdleConnections,
	}
	rows.startCancelWatch()

	// 结果下载已经建连，此时若 ctx 已取消，必须自己关掉响应体：
	// Rows 还没有交给调用方，没有人会再 Close 它。
	if err := ctx.Err(); err != nil {
		_ = rows.Close()
		return nil, instanceError(job, err)
	}

	return rows, nil
}

func instanceError(job *odps.SQLJob, err error) error {
	if job != nil {
		if ctxErr := contextCancellation(err); ctxErr != nil {
			return &CanceledError{InstanceID: job.Ref().InstanceID, Err: ctxErr}
		}
	}

	return errors.WithStack(err)
}

// contextCancellation 从错误链里取出 context.Canceled / context.DeadlineExceeded，
// 没有则返回 nil。
func contextCancellation(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}

	return nil
}

// ExecContext uses ctx for SQL submission and waiting for the instance.
func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	sqlStr, err := bindSQL(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.execContext(ctx, sqlStr)
}

func (c *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	sqlStr, err := positionArgQueryToSql(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.execContext(context.Background(), sqlStr)
}

func (c *connection) execContext(ctx context.Context, query string) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	job, err := c.submit(ctx, query)
	if err != nil {
		return nil, instanceError(job, err)
	}
	if _, err := job.Wait(ctx); err != nil {
		return nil, instanceError(job, err)
	}

	// odps 不报告受影响行数，但也不能返回 nil：database/sql 会把它原样交给调用方，
	// res.RowsAffected() 就 panic 在 nil interface 上。
	return NoResult{}, nil
}

func (c *connection) submit(ctx context.Context, query string) (*odps.SQLJob, error) {
	return c.connector.client.SubmitSQL(ctx, odps.SQLRequest{
		Statement: query,
		Options:   options.SQLTaskOptions{Hints: c.connector.options.Hints},
	})
}

func positionArgQueryToSql(query string, args []driver.Value) (string, error) {
	values := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		values[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return bindSQL(query, values)
}

// CheckNamedValue preserves exact MaxCompute decimal values for the binder.
func (c *connection) CheckNamedValue(value *driver.NamedValue) error {
	switch value.Value.(type) {
	case data.Decimal, *data.Decimal:
		return nil
	default:
		return driver.ErrSkip
	}
}

// Ping verifies that the current credentials can read the configured project.
func (c *connection) Ping(ctx context.Context) error {
	return c.connector.client.Ping(ctx)
}

var (
	_ driver.Pinger            = (*connection)(nil)
	_ driver.NamedValueChecker = (*connection)(nil)
)
