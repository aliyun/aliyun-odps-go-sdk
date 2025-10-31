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
	"bytes"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

type RecordPackStreamWriter struct {
	session      *StreamUploadSession
	protocWriter RecordProtocWriter
	flushing     bool
	buffer       *bytes.Buffer
	recordCount  int64
	appendErr    error
}

func newRecordStreamHttpWriter(session *StreamUploadSession) RecordPackStreamWriter {
	buffer := bytes.NewBuffer(nil)

	return RecordPackStreamWriter{
		session:      session,
		buffer:       buffer,
		protocWriter: newRecordProtocWriter(&bufWriter{buffer}, session.schema.Columns, false),
	}
}

func (rsw *RecordPackStreamWriter) Append(record data.Record) error {
	if rsw.flushing {
		rsw.appendErr = errors.New("There's an unsuccessful flush called, you should call flush to retry or call Reset to drop the data")
		return rsw.appendErr
	}
	if !rsw.session.allowSchemaMismatch {
		err := checkIfRecordSchemaMatchSessionSchema(&record, rsw.session.schema.Columns)
		if err != nil {
			rsw.appendErr = err
			return errors.WithStack(err)
		}
	}
	err := rsw.protocWriter.Write(record)
	if err == nil {
		rsw.recordCount += 1
	} else {
		rsw.appendErr = err
	}

	return errors.WithStack(err)
}

func checkIfRecordSchemaMatchSessionSchema(record *data.Record, schema []tableschema.Column) error {
	if record.Len() != len(schema) {
		return errors.Errorf("Record schema not match session schema, record len: %d, session schema len: %d",
			record.Len(), len(schema))
	}
	for index, recordData := range *record {
		colType := schema[index].Type.ID()
		if recordData != nil && recordData.Type() != datatype.NullType && recordData.Type().ID() != colType {
			return errors.Errorf("Record schema not match session schema, index: %d, record type: %s, session schema type: %s",
				index, recordData.Type().Name(), schema[index].Type.Name())
		}
	}
	return nil
}

// Flush send all buffered data to server. return (traceId, recordCount, recordBytes, error)
// `recordCount` and `recordBytes` is the count and bytes count of the records uploaded
func (rsw *RecordPackStreamWriter) Flush(timeout_ ...time.Duration) (string, int64, int64, error) {
	if rsw.appendErr != nil {
		return "", 0, 0, errors.New("There's an unsuccessful append called before, you should call Reset to drop the data and re-append the data.")
	}

	timeout := time.Duration(0)
	if len(timeout_) > 0 {
		timeout = timeout_[0]
	}

	if rsw.recordCount == 0 {
		return "", 0, 0, nil
	}

	// close protoc stream writer， the protoc stream will write the last protoc tags
	if (!rsw.flushing) && (!rsw.protocWriter.closed) {
		err := rsw.protocWriter.Close()
		if err != nil {
			return "", 0, 0, errors.WithStack(err)
		}
	}

	rsw.flushing = true

	reqId, bytesSend, err := rsw.session.flushStream(rsw, timeout)
	if err != nil {
		return "", 0, 0, err
	}

	recordCount := rsw.recordCount
	rsw.flushing = false
	rsw.Reset()

	return reqId, recordCount, int64(bytesSend), nil
}

// RecordCount the buffered record count
func (rsw *RecordPackStreamWriter) RecordCount() int64 {
	return rsw.recordCount
}

// DataSize the buffered data size
func (rsw *RecordPackStreamWriter) DataSize() int64 {
	return int64(rsw.buffer.Len())
}

func (rsw *RecordPackStreamWriter) Reset() {
	rsw.buffer.Reset()
	rsw.protocWriter = newRecordProtocWriter(&bufWriter{rsw.buffer}, rsw.session.schema.Columns, false)
	rsw.recordCount = 0
	rsw.appendErr = nil
}
