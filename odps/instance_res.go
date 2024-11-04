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

package odps

import (
	"encoding/base64"
	"encoding/xml"
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type TaskStatus int

const (
	_ = iota
	TaskWaiting
	TaskRunning
	TaskSuccess
	TaskFailed
	TaskSuspended
	TaskCancelled
	TaskStatusUnknown
)

func TaskStatusFromStr(s string) TaskStatus {
	switch strings.ToUpper(s) {
	case "WAITING":
		return TaskWaiting
	case "RUNNING":
		return TaskRunning
	case "SUCCESS":
		return TaskSuccess
	case "FAILED":
		return TaskFailed
	case "SUSPENDED":
		return TaskSuspended
	case "CANCELLED":
		return TaskCancelled
	default:
		return TaskStatusUnknown
	}
}

func (status TaskStatus) String() string {
	switch status {
	case TaskWaiting:
		return "WAITING"
	case TaskRunning:
		return "RUNNING"
	case TaskSuccess:
		return "SUCCESS"
	case TaskFailed:
		return "FAILED"
	case TaskSuspended:
		return "SUSPENDED"
	case TaskCancelled:
		return "CANCELLED"
	default:
		return "TASK_STATUS_UNKNOWN"
	}
}

// TaskInInstance 通过Instance创建的Task
type TaskInInstance struct {
	Type      string `xml:"Type,attr"`
	Name      string
	StartTime common.GMTTime
	EndTime   common.GMTTime `xml:"EndTime"`
	Status    TaskStatus
}

func (status *TaskStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	if err := d.DecodeElement(&s, &start); err != nil {
		return errors.WithStack(err)
	}

	*status = TaskStatusFromStr(s)

	return nil
}

func (status *TaskStatus) MarshalXML(d *xml.Encoder, start xml.StartElement) error {
	s := status.String()
	return errors.WithStack(d.EncodeElement(s, start))
}

type TaskProgressStage struct {
	ID                 string `xml:"ID,attr"`
	Status             string
	BackupWorkers      string
	TerminatedWorkers  string
	RunningWorkers     string
	TotalWorkers       string
	InputRecords       int
	OutRecords         int
	FinishedPercentage int
}

type TaskSummary struct {
	JsonSummary string
	Summary     string
}

type TaskResult struct {
	Type   string `xml:"Type,attr"`
	Name   string
	Status TaskStatus
	Result struct {
		TransForm string `xml:"Transform,attr"`
		Format    string `xml:"Format,attr"` // 这个字段没有用到
		Content   string `xml:",cdata"`
	} `xml:"Result"`
}

func (tr *TaskResult) Content() string {
	if tr.Result.TransForm == "Base64" {
		bytes, _ := base64.StdEncoding.DecodeString(tr.Result.Content)
		return string(bytes)
	}

	return tr.Result.Content
}
