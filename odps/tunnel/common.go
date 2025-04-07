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
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func addCommonSessionHttpHeader(header http.Header) {
	header.Add(common.HttpHeaderOdpsDateTransFrom, DateTransformVersion)
	header.Add(common.HttpHeaderOdpsTunnelVersion, Version)
}

type columnResModel struct {
	ColumnId     string `json:"column_id"`
	Comment      string `json:"comment"`
	DefaultValue string `json:"default_value"`
	Name         string `json:"name"`
	Nullable     bool   `json:"nullable"`
	Type         string `json:"type"`
}

type schemaResModel struct {
	IsVirtualView bool             `json:"IsVirtualView"`
	Columns       []columnResModel `json:"columns"`
	PartitionKeys []columnResModel `json:"partitionKeys"`
}

func (s *schemaResModel) toTableSchema(tableName string) (tableschema.TableSchema, error) {
	tableSchema := tableschema.TableSchema{
		TableName:     tableName,
		IsVirtualView: s.IsVirtualView,
	}

	tableSchema.Columns = make([]tableschema.Column, len(s.Columns))
	tableSchema.PartitionColumns = make([]tableschema.Column, len(s.PartitionKeys))

	toOdpsColumn := func(rawColumn columnResModel) (tableschema.Column, error) {
		_type, err := datatype.ParseDataType(rawColumn.Type)
		if err != nil {
			return tableschema.Column{}, errors.WithStack(err)
		}

		column := tableschema.Column{
			Name:         rawColumn.Name,
			Type:         _type,
			Comment:      rawColumn.Comment,
			NotNull:      !rawColumn.Nullable,
			DefaultValue: rawColumn.DefaultValue,
		}

		return column, nil
	}

	for i, rawColumn := range s.Columns {
		column, err := toOdpsColumn(rawColumn)
		if err != nil {
			return tableschema.TableSchema{}, errors.WithStack(err)
		}

		tableSchema.Columns[i] = column
	}

	for i, rawColumn := range s.PartitionKeys {
		column, err := toOdpsColumn(rawColumn)
		if err != nil {
			return tableschema.TableSchema{}, errors.WithStack(err)
		}

		tableSchema.PartitionColumns[i] = column
	}

	return tableSchema, nil
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func Retry(f func() error) error {
	// TODO: use tunnel retry strategy and add retry logger
	sleepTime := int64(1)
	var err error
	for i := 0; i < 3; i++ {
		err = f()
		if err == nil {
			break
		}

		sleepTime *= 1 << i
		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
	return err
}

type bufWriter struct {
	buf *bytes.Buffer
}

func (bw *bufWriter) Write(b []byte) (int, error) {
	return bw.buf.Write(b)
}

func (bw *bufWriter) Close() error {
	return nil
}

// 用于记录tunnel上传时http发送的数据量大小。对于压缩后的数据不能通过
// write方法返回的n来确定压缩后的数据大小。只能
// 1. 用bytesRecordWriter包装http conn
// 2. 用compressor writer包装bytesRecordWriter
// 3. compressor在压缩数据后，会调用bytesRecordWriter.write
// 4. bytesRecordWriter.write接收到的数据就是经过compressor压缩后的数据
// 5. bytesRecordWriter.write记录接收到的数据大小
type bytesRecordWriter struct {
	writer io.WriteCloser
	bytesN int
}

func newBytesRecordWriter(writer io.WriteCloser) *bytesRecordWriter {
	return &bytesRecordWriter{writer: writer}
}

func (brw *bytesRecordWriter) BytesN() int {
	return brw.bytesN
}

func (brw *bytesRecordWriter) Write(b []byte) (int, error) {
	n, err := brw.writer.Write(b)
	brw.bytesN += n
	return n, err
}

func (brw *bytesRecordWriter) Close() error {
	return brw.writer.Close()
}
