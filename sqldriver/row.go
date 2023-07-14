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
	"database/sql/driver"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/pkg/errors"
	"io"
	"reflect"
)

type rowsReader struct {
	columns []tableschema.Column
	inner   *tunnel.RecordProtocReader
}

func (rr *rowsReader) Columns() []string {
	columns := make([]string, len(rr.columns))

	for i, col := range rr.columns {
		columns[i] = col.Name
	}

	return columns
}

func (rr *rowsReader) Close() error {
	return errors.WithStack(rr.inner.Close())
}

func (rr *rowsReader) Next(dst []driver.Value) error {
	record, err := rr.inner.Read()

	if err == io.EOF {
		return err
	}

	if err != nil {
		return errors.WithStack(err)
	}

	if record.Len() != len(dst) {
		return errors.Errorf("expect %d columns, but get %d", len(dst), record.Len())
	}

	for i := range dst {
		dst[i] = record.Get(i)
	}

	return nil
}

func (rr *rowsReader) ColumnTypeDatabaseTypeName(index int) string {
	return rr.columns[index].Type.Name()
}

func (rr *rowsReader) ColumnTypeScanType(index int) reflect.Type {
	dataType := rr.columns[index].Type

	switch dataType.ID() {
	case datatype.BIGINT:
		return reflect.TypeOf(data.BigInt(0))
	case datatype.DOUBLE:
		return reflect.TypeOf(data.Double(0.0))
	case datatype.BOOLEAN:
		return reflect.TypeOf(data.Bool(false))
	case datatype.DATETIME:
		return reflect.TypeOf(data.DateTime{})
	case datatype.STRING:
		return reflect.TypeOf(data.String(""))
	case datatype.DECIMAL:
		return reflect.TypeOf(data.Decimal{})
	case datatype.MAP:
		return reflect.TypeOf(data.Map{})
	case datatype.ARRAY:
		return reflect.TypeOf(data.Array{})
	case datatype.VOID:
		return reflect.TypeOf(data.Null)
	case datatype.TINYINT:
		return reflect.TypeOf(data.TinyInt(0))
	case datatype.SMALLINT:
		return reflect.TypeOf(data.SmallInt(0))
	case datatype.INT:
		return reflect.TypeOf(data.Int(0))
	case datatype.FLOAT:
		return reflect.TypeOf(data.Float(0))
	case datatype.CHAR:
		return reflect.TypeOf(data.Char{})
	case datatype.VARCHAR:
		return reflect.TypeOf(data.VarChar{})
	case datatype.DATE:
		return reflect.TypeOf(data.Date{})
	case datatype.TIMESTAMP:
		return reflect.TypeOf(data.Timestamp{})
	case datatype.BINARY:
		return reflect.TypeOf(data.Binary{})
	case datatype.IntervalDayTime:
		return reflect.TypeOf(data.IntervalDayTime{})
	case datatype.IntervalYearMonth:
		return reflect.TypeOf(data.IntervalYearMonth(0))
	case datatype.STRUCT:
		return reflect.TypeOf(data.Struct{})
	}

	return nil
}
