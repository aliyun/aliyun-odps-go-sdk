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
	"errors"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"io"
	"testing"
)

type cancelAtEOFReader struct{ cancel context.CancelFunc }

func (r *cancelAtEOFReader) Read() (data.Record, error) { r.cancel(); return nil, io.EOF }
func (r *cancelAtEOFReader) Close() error               { return nil }

func TestRowsCancellationDuringReadIsNotEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows := &rowsReader{ctx: ctx, inner: &cancelAtEOFReader{cancel: cancel}}
	err := rows.Next(make([]driver.Value, 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
