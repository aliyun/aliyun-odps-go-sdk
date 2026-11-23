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
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// recordReader 是 rowsReader 需要的结果读取能力，*tunnel.RecordProtocReader 是它的
// 真实实现。用接口的目的有两个：sqldriver 不再依赖 tunnel 的具体类型，以及回归测试
// 可以塞进一个可控的假 reader（阻塞在 Read 上、统计 Close 次数）。
type recordReader interface {
	Read() (data.Record, error)
	Close() error
}

type rowsReader struct {
	columns []tableschema.Column
	inner   recordReader
	// ctx 是查询提交时拿到的 context，用于在两行之间发现取消。
	ctx context.Context
	// closed 用 atomic 而不是 mutex：Close 可能来自取消监听 goroutine，
	// 与正在进行中的 Read 并发，加锁会把两者串起来。
	closed int32
	// stopWatch 由 startCancelWatch 写入一次，Close 通过 watchOnce 关掉它。
	stopWatch chan struct{}
	watchOnce sync.Once
	// releaseIdleConns 关掉这一次结果下载用完的 keep-alive 连接。tunnel 每建一个
	// session 都会新起一个 http.Transport，不释放的话它的连接和喂它的 goroutine
	// 会随查询次数线性堆积。
	releaseIdleConns func()
}

// isClosed 报告结果集是否已经关闭。
func (rr *rowsReader) isClosed() bool {
	return atomic.LoadInt32(&rr.closed) == 1
}

// contextErr 返回 ctx 的取消原因，没有 ctx 或未取消时返回 nil。
func (rr *rowsReader) contextErr() error {
	if rr.ctx == nil {
		return nil
	}

	return rr.ctx.Err()
}

// startCancelWatch 在 ctx 取消时主动关闭结果流。这一步必须由 driver 来做：
// database/sql 在 ctx 取消时想关掉 Rows，需要拿到 Next 正持有着的锁，
// 而 Read 卡在网络上的 Next 永远不会放锁 —— 也就是说光靠 database/sql
// 取消不掉一个卡住的读。有了这个 goroutine，阻塞中的 Read 会因为响应体被
// 关闭而返回错误，Next 随即把 ctx.Err() 报给调用方。
// 不可取消的 context（Done() 为 nil）不会起 goroutine。
func (rr *rowsReader) startCancelWatch() {
	if rr.ctx == nil || rr.ctx.Done() == nil {
		return
	}

	stop := make(chan struct{})
	rr.stopWatch = stop

	go func() {
		select {
		case <-rr.ctx.Done():
			_ = rr.Close()
		case <-stop:
		}
	}()
}

// stopCancelWatch 结束取消监听：Rows 已经关了，再监听没有意义，
// 否则每查一次就漏一个 goroutine。
func (rr *rowsReader) stopCancelWatch() {
	rr.watchOnce.Do(func() {
		if rr.stopWatch != nil {
			close(rr.stopWatch)
		}
	})
}

func (rr *rowsReader) Columns() []string {
	columns := make([]string, len(rr.columns))

	for i, col := range rr.columns {
		columns[i] = col.Name
	}

	return columns
}

// Close 关闭底层结果流。重复 Close 返回 nil：database/sql 在 ctx 取消时会自己关掉
// Rows，调用方随后再 Close(rows.Close/defer) 是正常写法，不应该报错；底层响应体
// 也只应被关闭一次。
func (rr *rowsReader) Close() error {
	if !atomic.CompareAndSwapInt32(&rr.closed, 0, 1) {
		return nil
	}

	rr.stopCancelWatch()

	var err error
	if rr.inner != nil {
		err = errors.WithStack(rr.inner.Close())
	}

	if rr.releaseIdleConns != nil {
		rr.releaseIdleConns()
	}

	return err
}

func (rr *rowsReader) Next(dst []driver.Value) error {
	// 取消必须报成错误而不是 io.EOF：截断的结果集不能看起来像读完了。
	if err := rr.contextErr(); err != nil {
		return errors.WithStack(err)
	}

	// 已关闭的 Rows 上没有更多数据。返回 io.EOF 而不是 panic 或底层错误，
	// 这是 database/sql 认定“读完了”的信号。
	if rr.isClosed() || rr.inner == nil {
		return io.EOF
	}

	record, err := rr.inner.Read()

	// Cancellation can arrive while Read is blocked, including when the reader
	// reports EOF as it closes. Preserve the cancellation instead of reporting
	// a successfully drained result set.
	if ctxErr := rr.contextErr(); ctxErr != nil {
		return errors.WithStack(ctxErr)
	}

	if errors.Is(err, io.EOF) {
		return io.EOF
	}

	if err != nil {
		return errors.WithStack(err)
	}

	if record.Len() != len(dst) {
		return errors.Errorf("expect %d columns, but get %d", len(dst), record.Len())
	}

	for i, value := range record {
		switch v := value.(type) {
		case nil, data.NullData:
			dst[i] = nil
		case data.BigInt:
			dst[i] = int64(v)
		case data.Int:
			dst[i] = int64(v)
		case data.SmallInt:
			dst[i] = int64(v)
		case data.TinyInt:
			dst[i] = int64(v)
		case data.Double:
			dst[i] = float64(v)
		case data.Float:
			dst[i] = float64(v)
		case data.String:
			dst[i] = string(v)
		case data.Char:
			dst[i] = v.Data()
		case data.VarChar:
			dst[i] = v.Data()
		case data.Binary:
			dst[i] = []byte(v)
		case data.Bool:
			dst[i] = bool(v)
		case data.DateTime:
			dst[i] = time.Time(v)
		case data.Date:
			dst[i] = time.Time(v)
		case data.Timestamp:
			dst[i] = time.Time(v)
		case data.TimestampNtz:
			dst[i] = time.Time(v)
		case data.Decimal:
			dst[i] = decimalValue(&v)
		case *data.Decimal:
			dst[i] = decimalValue(v)
		default:
			// Complex values retain their schema and use the SDK Scanner types.
			dst[i] = value
		}
	}

	return nil
}

func decimalValue(value *data.Decimal) driver.Value {
	if value == nil || !value.Valid {
		return nil
	}
	return value.String()
}

func (rr *rowsReader) ColumnTypeDatabaseTypeName(index int) string {
	return rr.columns[index].Type.Name()
}

// ColumnTypePrecisionScale exposes the declared DECIMAL type through sql.ColumnType.DecimalSize.
func (rr *rowsReader) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	typ, ok := rr.columns[index].Type.(datatype.DecimalType)
	return int64(typ.Precision), int64(typ.Scale), ok
}

func (rr *rowsReader) ColumnTypeScanType(index int) reflect.Type {
	column := rr.columns[index]
	dataType := column.Type
	nullable := !column.NotNull

	switch dataType.ID() {
	case datatype.BIGINT:
		if nullable {
			return reflect.TypeOf(NullInt64{})
		}

		return reflect.TypeOf(int64(0))
	case datatype.INT:
		if nullable {
			return reflect.TypeOf(NullInt32{})
		}

		return reflect.TypeOf(int64(0))
	case datatype.SMALLINT:
		if nullable {
			return reflect.TypeOf(NullInt16{})
		}

		return reflect.TypeOf(int64(0))
	case datatype.TINYINT:
		if nullable {
			return reflect.TypeOf(NullInt8{})
		}

		return reflect.TypeOf(int64(0))
	case datatype.DOUBLE:
		if nullable {
			return reflect.TypeOf(NullFloat64{})
		}

		return reflect.TypeOf(float64(0))
	case datatype.FLOAT:
		if nullable {
			return reflect.TypeOf(NullFloat32{})
		}

		return reflect.TypeOf(float64(0))

	case datatype.STRING, datatype.CHAR, datatype.VARCHAR:
		if nullable {
			return reflect.TypeOf(NullString{})
		}

		return reflect.TypeOf("")
	case datatype.BINARY:
		return reflect.TypeOf(Binary{})
	case datatype.BOOLEAN:
		if nullable {
			return reflect.TypeOf(NullBool{})
		}

		return reflect.TypeOf(false)
	case datatype.DATETIME:
		return reflect.TypeOf(NullDateTime{})
	case datatype.DATE:
		return reflect.TypeOf(NullDate{})
	case datatype.TIMESTAMP:
		return reflect.TypeOf(NullTimeStamp{})
	case datatype.TIMESTAMP_NTZ:
		return reflect.TypeOf(NullTimeStampNtz{})
	case datatype.DECIMAL:
		return reflect.TypeOf(Decimal{})
	case datatype.MAP:
		return reflect.TypeOf(Map{})
	case datatype.ARRAY:
		return reflect.TypeOf(Array{})
	case datatype.STRUCT:
		return reflect.TypeOf(Struct{})
	case datatype.JSON:
		return reflect.TypeOf(Json{})
	case datatype.VOID:
		return reflect.TypeOf(data.Null)
	case datatype.IntervalDayTime:
		return reflect.TypeOf(data.IntervalDayTime{})
	case datatype.IntervalYearMonth:
		return reflect.TypeOf(data.IntervalYearMonth(0))
	}

	return nil
}
