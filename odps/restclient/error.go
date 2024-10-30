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
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
)

type HttpError struct {
	Status       string
	StatusCode   int
	RequestId    string
	Body         []byte
	ErrorMessage *ErrorMessage
	Response     *http.Response
}

type ErrorMessage struct {
	ErrorCode string `json:"Code" xml:"Code"`
	Message   string `json:"Message" xml:"Message"`
	RequestId string `json:"RequestId" xml:"RequestId"`
	HostId    string `json:"HostId" xml:"HostId"`
}

func (e HttpError) Error() string {
	if e.RequestId == "" {
		return fmt.Sprintf("%s\n%s", e.Status, e.Body)
	}

	return fmt.Sprintf("requestId=%s\nstatus=%s\n%s", e.RequestId, e.Status, e.Body)
}

func NewHttpNotOk(res *http.Response) HttpError {
	var body []byte

	if res.Body != nil {
		body, _ = ioutil.ReadAll(res.Body)
		_ = res.Body.Close()
	}

	return HttpError{
		Status:       res.Status,
		StatusCode:   res.StatusCode,
		RequestId:    res.Header.Get("x-odps-request-id"),
		Body:         body,
		ErrorMessage: NewErrorMessage(body),
		Response:     res,
	}
}

func NewErrorMessage(body []byte) *ErrorMessage {
	if body == nil {
		return nil
	}

	var errorMessage ErrorMessage

	// 尝试解析为 XML
	if err := xml.Unmarshal(body, &errorMessage); err == nil {
		return &errorMessage
	}

	// 尝试解析为 JSON
	if err := json.Unmarshal(body, &errorMessage); err == nil {
		return &errorMessage
	}

	// 如果都失败了，返回 nil
	return nil
}
