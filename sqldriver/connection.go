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

// Begin sql/driver.Conn接口实现，由于odps不支持实物，方法的实现为空
func (c *connection) Begin() (driver.Tx, error) {
	return nil, nil
}

// Prepare sql/driver.Conn接口实现，由于odps不支持prepare statement, 方法实现为空p
func (c *connection) Prepare(string) (driver.Stmt, error) {
	return nil, nil
}

// Close sql/driver.Conn接口实现。odps 的 REST 调用本身没有服务端会话状态要清理，
// 但 RestClient 的 transport 会留着 keep-alive 连接和喂它们的 goroutine：
// database/sql 丢掉这条连接时把它们关掉。
func (c *connection) Close() error {
	restClient := c.odpsIns.RestClient()
	restClient.CloseIdleConnections()

	return nil
}

// QueryContext sql/driver.QueryerContext接口实现
//
// ctx 控制客户端的等待与资源：提交前已取消则不去创建远端 instance；等待期间
// 取消/超时会尽快返回 ctx.Err()；返回 Rows 之后 ctx 也管到行消费 —— 取消时
// driver 会关掉结果流，卡住的 Next 立即带着 ctx.Err() 返回。
//
// ctx 不会终止服务端的 MaxCompute instance：终止运行中的任务是破坏性动作，
// 由调用方决定（见 odps.Instance.Terminate）；取消报错里带 instance id 就是为了
// 让调用方还能追到这个任务。已经发出的单个 HTTP 请求不会被打断，所以等待的
// 返回时刻在一个轮询周期加一次往返之内。
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

	// 执行sql task，获取instance
	ins, err := c.odpsIns.ExecSQlWithHints(query, c.config.Hints)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 等待instance结束，等待受ctx控制
	if err = ins.WaitForSuccessContext(ctx); err != nil {
		return nil, waitError(ins, err)
	}

	// 如果dsn中配置了enableLogview=true，将打印相应logView
	if value, ok := c.config.Others["enableLogview"]; ok && strings.ToLower(value) == "true" {
		lv := c.odpsIns.LogView()
		lvUrl, err := lv.GenerateLogView(ins, 10)
		if err != nil {
			return nil, errors.Wrapf(err, "Generate logView failed.")
		}

		log.Printf("%s\n", lvUrl)
	}

	// 调用instance tunnel, 下载结果
	tunnelEndpoint := c.config.TunnelEndpoint
	if tunnelEndpoint != "" && c.config.TunnelQuotaName != "" {
		return nil, errors.New(`"tunnelEndpoint" and "tunnelQuotaName" cannot be configured both`)
	}

	if c.config.TunnelQuotaName != "" {
		project := c.odpsIns.DefaultProject()
		tunnelEndpoint, err = project.GetTunnelEndpoint(c.config.TunnelQuotaName)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if tunnelEndpoint == "" {
		project := c.odpsIns.DefaultProject()
		tunnelEndpoint, err = project.GetTunnelEndpoint()
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, canceledWaitingInstance(ins, err)
	}

	// 结果下载的连接是一次性的：每建一个 session 都会新起一个 http.Transport，
	// 池化那条 keep-alive 连接没有下一次可用，只会连同喂它的 goroutine 一起留下，
	// 所以这里显式关掉 keep-alive（见 tunnel.Tunnel.DisableKeepAlives）。
	tunnelIns := tunnel.NewTunnel(c.odpsIns, tunnelEndpoint)
	tunnelIns.DisableKeepAlives = true
	projectName := c.odpsIns.DefaultProjectName()
	session, err := tunnelIns.CreateInstanceResultDownloadSession(projectName, ins.Id())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	recordCount := session.RecordCount()
	if recordCount == 0 {
		recordCount = 1
	}

	reader, err := session.OpenRecordReader(0, recordCount, 0, nil)
	if err != nil {
		return nil, errors.WithStack(err)
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
		return nil, canceledWaitingInstance(ins, err)
	}

	return rows, nil
}

// waitError 把等待 instance 结束的错误转成 driver 层的错误。ctx 取消/超时时
// 带上 instance id：调用方只有拿 id 才能在服务端继续跟踪或主动终止该任务。
func waitError(ins *odps.Instance, err error) error {
	if ctxErr := contextCancellation(err); ctxErr != nil {
		return canceledWaitingInstance(ins, ctxErr)
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

func canceledWaitingInstance(ins *odps.Instance, ctxErr error) error {
	return &CanceledError{InstanceID: ins.Id(), Err: ctxErr}
}

// ExecContext sql/driver.ExecerContext接口实现，ctx 语义与 QueryContext 一致：
// 只约束客户端等待，不终止服务端 instance。
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

	// 执行sql task，获取instance
	ins, err := c.odpsIns.ExecSQlWithHints(query, c.config.Hints)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 等待instance结束
	if err = ins.WaitForSuccessContext(ctx); err != nil {
		return nil, waitError(ins, err)
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
	positionArgQuery := NewPositionArgQuery(query)
	for _, arg := range args {
		positionArgQuery.SetArgs(arg)
	}

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
