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

package tunnel

import (
	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
)

type RecordArrowWriter struct {
	conn              *httpConnection
	schema            *arrow.Schema
	recordBatchWriter *ipc.RecordBatchWriter
	httpWriter        *ArrowStreamWriter
	closed            bool
}

func newRecordArrowWriter(conn *httpConnection, schema *arrow.Schema) RecordArrowWriter {
	httpWriter := NewArrowStreamWriter(conn.Writer)

	return RecordArrowWriter{
		conn:              conn,
		schema:            schema,
		recordBatchWriter: ipc.NewRecordBatchWriter(httpWriter, ipc.WithSchema(schema)),
		httpWriter:        httpWriter,
	}
}

func (writer *RecordArrowWriter) WriteArrowRecord(record array.Record) error {
	return writer.recordBatchWriter.Write(record)
}

func (writer *RecordArrowWriter) Close() error {
	if writer.closed {
		return errors.New("try to close a closed RecordArrowWriter")
	}

	writer.closed = true
	err := writer.close()

	httpCloseErr := errors.WithStack(writer.conn.closeRes())
	if httpCloseErr != nil {
		return errors.WithStack(httpCloseErr)
	}

	return errors.WithStack(err)
}

func (writer *RecordArrowWriter) close() error {
	err1 := writer.recordBatchWriter.Close()
	err2 := writer.httpWriter.Close()

	if err1 != nil {
		return errors.WithStack(err1)
	}

	if err2 != nil {
		return errors.WithStack(err2)
	}

	return nil
}
