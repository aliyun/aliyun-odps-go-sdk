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

	if errors.Is(err, io.EOF) {
		return io.EOF
	}

	if err != nil {
		// 取消时关流会让进行中的 Read 报错，这里把真实原因换成 ctx 的错误。
		if ctxErr := rr.contextErr(); ctxErr != nil {
			return errors.WithStack(ctxErr)
		}

		return errors.WithStack(err)
	}

	if record.Len() != len(dst) {
		return errors.Errorf("expect %d columns, but get %d", len(dst), record.Len())
	}

	for i := range dst {
		ri := record.Get(i)
		dst[i] = ri

		if ri == nil {
			continue
		}

		switch ri.Type().ID() {
		case datatype.BIGINT:
			dst[i] = int64(ri.(data.BigInt))
		case datatype.INT:
			dst[i] = int(ri.(data.Int))
		case datatype.SMALLINT:
			dst[i] = int16(ri.(data.SmallInt))
		case datatype.TINYINT:
			dst[i] = int8(ri.(data.TinyInt))
		case datatype.DOUBLE:
			dst[i] = float64(ri.(data.Double))
		case datatype.FLOAT:
			dst[i] = float32(ri.(data.Float))
		case datatype.STRING:
			dst[i] = string(ri.(data.String))
		case datatype.CHAR:
			char := ri.(data.Char)
			dst[i] = char.Data()
		case datatype.VARCHAR:
			char := ri.(data.VarChar)
			dst[i] = char.Data()
		case datatype.BINARY:
			dst[i] = []byte(ri.(data.Binary))
		case datatype.BOOLEAN:
			dst[i] = bool(ri.(data.Bool))
		case datatype.DATETIME:
			dst[i] = time.Time(ri.(data.DateTime))
		case datatype.DATE:
			dst[i] = time.Time(ri.(data.Date))
		case datatype.TIMESTAMP:
			dst[i] = time.Time(ri.(data.Timestamp))
		case datatype.TIMESTAMP_NTZ:
			dst[i] = time.Time(ri.(data.TimestampNtz))
		// case datatype.DECIMAL:
		//	dst[i] = ri
		// case datatype.MAP:
		//	dst[i] = ri
		// case datatype.ARRAY:
		//	dst[i] = ri
		// case datatype.STRUCT:
		//	dst[i] = ri
		// case datatype.VOID:
		//	dst[i] = ri
		// case datatype.IntervalDayTime:
		//	dst[i] = ri
		// case datatype.IntervalYearMonth:
		//	dst[i] = ri
		default:
			dst[i] = ri
		}
	}

	return nil
}

func (rr *rowsReader) ColumnTypeDatabaseTypeName(index int) string {
	return rr.columns[index].Type.Name()
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

		return reflect.TypeOf(int(0))
	case datatype.SMALLINT:
		if nullable {
			return reflect.TypeOf(NullInt16{})
		}

		return reflect.TypeOf(int16(0))
	case datatype.TINYINT:
		if nullable {
			return reflect.TypeOf(NullInt8{})
		}

		return reflect.TypeOf(int8(0))
	case datatype.DOUBLE:
		if nullable {
			return reflect.TypeOf(NullFloat64{})
		}

		return reflect.TypeOf(float64(0))
	case datatype.FLOAT:
		if nullable {
			return reflect.TypeOf(NullFloat32{})
		}

		return reflect.TypeOf(float32(0))

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
