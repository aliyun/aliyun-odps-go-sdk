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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/pkg/errors"
	"io"
)

type rowsReader struct {
	columns []string
	inner   *tunnel.RecordProtocReader
}

func (rr *rowsReader) Columns() []string {
	return rr.columns
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
