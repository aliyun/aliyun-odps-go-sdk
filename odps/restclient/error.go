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

package restclient

import (
	"fmt"
	"io"
	"net/http"
)

type HttpNotOk struct {
	Status     string
	StatusCode int
	RequestId  string
	Body       []byte
}

func (e HttpNotOk) Error() string {
	if e.RequestId == "" {
		return fmt.Sprintf("%s\n%s", e.Status, e.Body)
	}

	return fmt.Sprintf("requestId=%s\nstatus=%s\n%s", e.RequestId, e.Status, e.Body)
}

type NoSuchObject struct {
	RequestId string
	Body      []byte
}

func (e NoSuchObject) Error() string {
	if e.RequestId == "" {
		return fmt.Sprintf("%s\n%s", "No such object.", e.Body)
	}

	return fmt.Sprintf("requestId=%s\nstatus=%s\n%s", e.RequestId, "No such object.", e.Body)
}

func NewHttpNotOk(res *http.Response) error {
	var body []byte

	if res.Body != nil {
		body, _ = io.ReadAll(res.Body)
		_ = res.Body.Close()
	}

	if res.StatusCode == http.StatusNotFound {
		return NoSuchObject{
			RequestId: res.Header.Get("x-odps-request-id"),
			Body:      body,
		}
	}

	return HttpNotOk{
		Status:     res.Status,
		StatusCode: res.StatusCode,
		RequestId:  res.Header.Get("x-odps-request-id"),
		Body:       body,
	}
}
