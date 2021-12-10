package sqldriver

import (
	"context"
	"database/sql/driver"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/tunnel"
	"github.com/pkg/errors"
)

type connection struct {
	odpsIns *odps.Odps
}

func newConnection(odpsIns *odps.Odps) *connection {
	return &connection{odpsIns: odpsIns}
}

// Begin sql/driver.Conn接口实现，由于odps不支持实物，方法的实现为空
func (c *connection) Begin() (driver.Tx, error) {
	return nil, nil
}

// Prepare sql/driver.Conn接口实现，由于odps不支持prepare statement, 方法实现为空p
func (c *connection) Prepare(string) (driver.Stmt, error) {
	return nil, nil
}

// Close sql/driver.Conn接口实现，由于odps通过rest接口获取数据, 一个rest连接只会用一次，所以无需关闭
func (c *connection) Close() error {
	return nil
}

// QueryContext sql/driver.QueryerContext接口实现
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlStr, err := namedArgQueryToSql(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.query(sqlStr)
}

func (c *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	sqlStr, err := positionArgQueryToSql(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.query(sqlStr)
}

func (c *connection) query(query string) (driver.Rows, error) {
	// 执行sql task，获取instance
	ins, err := c.odpsIns.RunSQl(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 等待instance结束
	err = ins.WaitForSuccess()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 调用instance tunnel, 下载结果
	tunnelIns, err := tunnel.NewTunnelFromProject(c.odpsIns.DefaultProject())
	if err != nil {
		return nil, err
	}

	projectName := c.odpsIns.DefaultProjectName()
	session, err := tunnelIns.CreateInstanceResultDownloadSession(projectName, ins.Id())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	recordCount := session.RecordCount()
	reader, err := session.OpenRecordReader(0, recordCount, 0, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	schema := session.Schema()
	columns := make([]string, len(schema.Columns))

	for i, col := range schema.Columns {
		columns[i] = col.Name
	}

	rows := &rowsReader{
		columns: columns,
		inner:   reader,
	}

	return rows, nil
}

func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	sqlStr, err := namedArgQueryToSql(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.exec(sqlStr)
}

func (c *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	sqlStr, err := positionArgQueryToSql(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return c.exec(sqlStr)
}

func (c *connection) exec(query string) (driver.Result, error) {
	// 执行sql task，获取instance
	ins, err := c.odpsIns.RunSQl(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 等待instance结束
	err = ins.WaitForSuccess()
	return nil, errors.WithStack(err)
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
func (c *connection) Ping(ctx context.Context) error {
	return driver.ErrBadConn
}

// IsValid Validator may be implemented by Conn to allow drivers to signal if a connection is valid or if it should be discarded.
// If implemented, drivers may return the underlying error from queries, even if the connection should be discarded by the connection pool.
func (c *connection) IsValid() bool {
	return false
}
