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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/pkg/errors"
	"time"
)

type RecordPackStreamWriter struct {
	session      *StreamUploadSession
	protocWriter RecordProtocWriter
	flushing     bool
	buffer       *bytes.Buffer
	recordCount  int64
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
		return errors.New("There's an unsuccessful flush called, you should call flush to retry or call reset to drop the data")
	}

	err := errors.WithStack(rsw.protocWriter.Write(record))

	if err != nil {
		rsw.recordCount += 1
	}

	return err
}

func (rsw *RecordPackStreamWriter) Flush(timeout_ ...time.Duration) (string, error) {
	rsw.flushing = true

	timeout := time.Duration(0)
	if len(timeout_) > 0 {
		timeout = timeout_[0]
	}

	reqId, err := rsw.session.flushStream(rsw, timeout)
	if err != nil {
		return "", errors.WithStack(err)
	}

	rsw.flushing = false
	rsw.reset()

	return reqId, nil
}

func (rsw *RecordPackStreamWriter) RecordCount() int64 {
	return rsw.recordCount
}

func (rsw *RecordPackStreamWriter) DataSize() int64 {
	return int64(rsw.buffer.Len())
}

func (rsw *RecordPackStreamWriter) reset() {
	rsw.buffer.Reset()
	rsw.recordCount = 0
}
