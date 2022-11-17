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
	"io"
	"net/http"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/ipc"
	"github.com/pkg/errors"
)

type RecordArrowReader struct {
	httpRes           *http.Response
	recordBatchReader *ipc.RecordBatchReader
	arrowReader       *ArrowStreamReader
}

func newRecordArrowReader(res *http.Response, schema *arrow.Schema) RecordArrowReader {
	httpReader := NewArrowStreamReader(res.Body)

	return RecordArrowReader{
		httpRes:           res,
		recordBatchReader: ipc.NewRecordBatchReader(httpReader, schema),
		arrowReader:       httpReader,
	}
}

func (r *RecordArrowReader) HttpRes() *http.Response {
	return r.httpRes
}

func (r *RecordArrowReader) RecordBatchReader() *ipc.RecordBatchReader {
	return r.recordBatchReader
}

func (r *RecordArrowReader) Iterator(f func(array.Record, error)) {
	for {
		record, err := r.recordBatchReader.Read()
		isEOF := errors.Is(err, io.EOF)
		if isEOF {
			return
		}
		if err != nil {
			f(record, err)
			return
		}

		record.Retain()
		f(record, err)
	}
}

func (r *RecordArrowReader) Read() (array.Record, error) {
	record, err := r.recordBatchReader.Read()
	return record, errors.WithStack(err)
}

func (r *RecordArrowReader) Close() error {
	r.recordBatchReader.Release()
	return errors.WithStack(r.arrowReader.Close())
}
