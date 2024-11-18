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

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

type httpConnection struct {
	Writer      io.WriteCloser
	resChan     <-chan resOrErr
	bytesRecord *bytesRecordWriter // 用于记录http上传的数据大小(压缩后的)
}

type resOrErr struct {
	err error
	res *http.Response
}

func newHttpConnection(writeCloser io.WriteCloser, resChan <-chan resOrErr, compressor Compressor) *httpConnection {
	bytesRecord := newBytesRecordWriter(writeCloser)
	var writer io.WriteCloser = bytesRecord
	if compressor != nil {
		writer = compressor.NewWriter(writer)
	}

	return &httpConnection{
		Writer:      writer,
		resChan:     resChan,
		bytesRecord: bytesRecord,
	}
}

func (conn *httpConnection) closeRes() error {
	rOrE := <-conn.resChan

	if rOrE.err != nil {
		return errors.WithStack(rOrE.err)
	}

	res := rOrE.res

	if res.StatusCode/100 != 2 {
		return errors.WithStack(restclient.NewHttpNotOk(res))
	}

	err := res.Body.Close()
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (conn *httpConnection) bytesCount() int {
	return conn.bytesRecord.BytesN()
}
