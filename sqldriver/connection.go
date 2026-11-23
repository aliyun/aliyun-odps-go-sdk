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
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

type connection struct {
	odpsIns *odps.Odps
	config  *odps.Config
}

func newConnection(config *odps.Config) *connection {
	return &connection{
		odpsIns: config.GenOdps(),
		config:  config,
	}
}

// Begin reports that MaxCompute does not support database/sql transactions.
func (c *connection) Begin() (driver.Tx, error) {
	return nil, ErrTransactionsUnsupported
}

// Prepare reports that the driver does not support prepared statements.
func (c *connection) Prepare(string) (driver.Stmt, error) {
	return nil, ErrPreparedStatementsUnsupported
}

// Close sql/driver.Conn接口实现。odps 的 REST 调用本身没有服务端会话状态要清理，
// 但 RestClient 的 transport 会留着 keep-alive 连接和喂它们的 goroutine：
// database/sql 丢掉这条连接时把它们关掉。
func (c *connection) Close() error {
	restClient := c.odpsIns.RestClient()
	restClient.CloseIdleConnections()

	return nil
}

// QueryContext uses ctx for submission, waiting, and reading results. An
// interrupted operation with a known instance ID returns CanceledError.
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlStr, err := namedArgQueryToSql(query, args)
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

	tunnelEndpoint := c.config.TunnelEndpoint
	if tunnelEndpoint != "" && c.config.TunnelQuotaName != "" {
		return nil, errors.New(`"tunnelEndpoint" and "tunnelQuotaName" cannot be configured both`)
	}

	ins, err := c.odpsIns.ExecSQLContext(ctx, query, c.config.Hints)
	if err != nil {
		return nil, instanceError(ins, err)
	}
	if err = ins.WaitForSuccessContext(ctx); err != nil {
		return nil, instanceError(ins, err)
	}

	// 如果dsn中配置了enableLogview=true，将打印相应logView
	if value, ok := c.config.Others["enableLogview"]; ok && strings.ToLower(value) == "true" {
		lvUrl, err := c.odpsIns.GenerateLogViewContext(ctx, ins, 10)
		if err != nil {
			return nil, instanceError(ins, errors.Wrap(err, "generate LogView"))
		}

		log.Printf("%s\n", lvUrl)
	}

	if tunnelEndpoint == "" {
		project := c.odpsIns.DefaultProject()
		tunnelEndpoint, err = project.GetTunnelEndpointContext(ctx, c.config.TunnelQuotaName)
		if err != nil {
			return nil, instanceError(ins, err)
		}
	}

	// 结果下载的连接是一次性的：每建一个 session 都会新起一个 http.Transport，
	// 池化那条 keep-alive 连接没有下一次可用，只会连同喂它的 goroutine 一起留下，
	// 所以这里显式关掉 keep-alive（见 tunnel.Tunnel.DisableKeepAlives）。
	tunnelIns := tunnel.NewTunnel(c.odpsIns, tunnelEndpoint)
	tunnelIns.SetQuotaName(c.config.TunnelQuotaName)
	tunnelIns.DisableKeepAlives = true
	projectName := c.odpsIns.DefaultProjectName()
	session, err := tunnelIns.CreateInstanceResultDownloadSessionContext(ctx, projectName, ins.Id())
	if err != nil {
		return nil, instanceError(ins, err)
	}

	recordCount := session.RecordCount()
	if recordCount == 0 {
		recordCount = 1
	}

	reader, err := session.OpenRecordReaderContext(ctx, 0, recordCount, 0, nil)
	if err != nil {
		return nil, instanceError(ins, err)
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
		return nil, instanceError(ins, err)
	}

	return rows, nil
}

func instanceError(ins *odps.Instance, err error) error {
	if ins != nil {
		if ctxErr := contextCancellation(err); ctxErr != nil {
			return &CanceledError{InstanceID: ins.Id(), Err: ctxErr}
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
	sqlStr, err := namedArgQueryToSql(query, args)
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

	ins, err := c.odpsIns.ExecSQLContext(ctx, query, c.config.Hints)
	if err != nil {
		return nil, instanceError(ins, err)
	}

	// 等待instance结束
	if err = ins.WaitForSuccessContext(ctx); err != nil {
		return nil, instanceError(ins, err)
	}

	// odps 不报告受影响行数，但也不能返回 nil：database/sql 会把它原样交给调用方，
	// res.RowsAffected() 就 panic 在 nil interface 上。
	return NoResult{}, nil
}

func namedArgQueryToSql(query string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	if args[0].Name == "" {
		values := make([]driver.Value, len(args))
		for i, arg := range args {
			values[i] = arg.Value
		}
		return positionArgQueryToSql(query, values)
	}

	namedArgQuery := NewNamedArgQuery(query)
	for _, arg := range args {
		namedArgQuery.SetArg(arg.Name, arg.Value)
	}

	return namedArgQuery.toSql()
}

func positionArgQueryToSql(query string, args []driver.Value) (string, error) {
	values := make([]any, len(args))
	for i, arg := range args {
		values[i] = arg
	}
	positionArgQuery := NewPositionArgQuery(query)
	positionArgQuery.SetArgs(values...)
	return positionArgQuery.toSql()
}

// Ping Pinger is an optional interface that may be implemented by a Conn.
// If a Conn does not implement Pinger, the sql package's DB.Ping and DB.PingContext will check if there is at least one Conn available.
// If Conn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove the Conn from pool.
//func (c *connection) Ping(ctx context.Context) error {
//	return driver.ErrBadConn
//}

// IsValid Validator may be implemented by Conn to allow drivers to signal if a connection is valid or if it should be discarded.
// If implemented, drivers may return the underlying error from queries, even if the connection should be discarded by the connection pool.
//func (c *connection) IsValid() bool {
//	return false
//}
