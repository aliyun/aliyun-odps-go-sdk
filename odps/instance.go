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
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

type InstanceStatus int

const (
	_ InstanceStatus = iota
	InstanceRunning
	InstanceSuspended
	InstanceTerminated
	InstanceStatusUnknown
)

const DefaultJobPriority = 9

type Instance struct {
	projectName       string
	id                string
	owner             string
	startTime         time.Time
	endTime           time.Time
	status            InstanceStatus
	odpsIns           *Odps
	reLoaded          bool
	resourceUrl       string
	taskNameCommitted string
	taskResults       []TaskResult
	isSync            bool
}

// InstanceOrErr is used for the return value of Instances.List
type InstanceOrErr struct {
	Ins *Instance
	Err error
}

func NewInstance(odpsIns *Odps, projectName, instanceId string) *Instance {
	rb := common.ResourceBuilder{ProjectName: projectName}

	return &Instance{
		id:          instanceId,
		projectName: projectName,
		odpsIns:     odpsIns,
		resourceUrl: rb.Instance(instanceId),
	}
}

func (instance *Instance) TaskNameCommitted() string {
	return instance.taskNameCommitted
}

func (instance *Instance) ProjectName() string {
	return instance.projectName
}

func (instance *Instance) IsLoaded() bool {
	return instance.reLoaded
}

func (instance *Instance) IsSync() bool {
	return instance.isSync
}

func (instance *Instance) IsAsync() bool {
	return !instance.isSync
}

func (instance *Instance) Load() error {
	client := instance.odpsIns.restClient

	type ResModel struct {
		XMLName xml.Name `xml:"Instance"`
		Status  InstanceStatus
	}
	var resModel ResModel

	err := client.GetWithParseFunc(instance.resourceUrl, nil, func(res *http.Response) error {
		header := res.Header
		instance.owner = header.Get(common.HttpHeaderOdpsOwner)
		instance.startTime, _ = common.ParseRFC1123Date(header.Get(common.HttpHeaderOdpsStartTime))
		instance.endTime, _ = common.ParseRFC1123Date(header.Get(common.HttpHeaderOdpsEndTime))

		decoder := xml.NewDecoder(res.Body)
		if err := decoder.Decode(&resModel); err != nil {
			return errors.WithStack(err)
		}

		instance.status = resModel.Status
		return nil
	})

	return errors.WithStack(err)
}

func (instance *Instance) Terminate() error {
	type BodyModel struct {
		XMLName xml.Name `xml:"Instance"`
		Status  InstanceStatus
	}
	var bodyModel = BodyModel{
		Status: InstanceTerminated,
	}

	client := instance.odpsIns.restClient
	err := client.DoXmlWithParseFunc("PUT", instance.resourceUrl, nil, nil, &bodyModel, nil)
	return errors.WithStack(err)
}

// GetTasks 绝大部分时候返回一个Task(名字与提交的task名字相同)，返回多个task的情况我还没有遇到过
func (instance *Instance) GetTasks() ([]TaskInInstance, error) {
	urlQuery := make(url.Values)
	urlQuery.Set("taskstatus", "")

	type ResModel struct {
		XMLName xml.Name         `xml:"Instance"`
		Tasks   []TaskInInstance `xml:"Tasks>Task"`
	}

	var resModel ResModel
	client := instance.odpsIns.restClient

	err := client.GetWithModel(instance.resourceUrl, urlQuery, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resModel.Tasks, nil
}

func (instance *Instance) GetTaskProgress(taskName string) ([]TaskProgressStage, error) {
	queryArgs := make(url.Values)
	queryArgs.Set("instanceprogress", "")
	queryArgs.Set("taskname", taskName)

	client := instance.odpsIns.restClient
	type ResModel struct {
		XMLName xml.Name            `xml:"Progress"`
		Stages  []TaskProgressStage `xml:"Stage"`
	}

	var resModel ResModel

	err := client.GetWithModel(instance.resourceUrl, queryArgs, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resModel.Stages, nil
}

func (instance *Instance) GetTaskDetail(taskName string) ([]byte, error) {
	queryArgs := make(url.Values, 2)
	queryArgs.Set("instancedetail", "")
	queryArgs.Set("taskname", taskName)

	client := instance.odpsIns.restClient
	var body []byte

	err := client.GetWithParseFunc(instance.resourceUrl, queryArgs, func(res *http.Response) error {
		var err error
		body, err = ioutil.ReadAll(res.Body)
		return errors.WithStack(err)
	})

	return body, errors.WithStack(err)
}

func (instance *Instance) GetTaskSummary(taskName string) (*TaskSummary, error) {
	queryArgs := make(url.Values, 2)
	queryArgs.Set("instancesummary", "")
	queryArgs.Set("taskname", taskName)

	client := instance.odpsIns.restClient
	type ResModel struct {
		Instance struct {
			JsonSummary string
			Summary     string
		}
	}

	var resModel ResModel

	err := client.GetWithParseFunc(instance.resourceUrl, queryArgs, func(res *http.Response) error {
		decoder := json.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&resModel))
	})

	if err != nil {
		return nil, errors.WithStack(err)
	}

	taskSummary := TaskSummary{
		JsonSummary: resModel.Instance.JsonSummary,
		Summary:     resModel.Instance.Summary,
	}

	return &taskSummary, errors.WithStack(err)
}

func (instance *Instance) GetTaskQuotaJson(taskName string) (string, error) {
	queryArgs := make(url.Values, 2)
	queryArgs.Set("instancequota", "")
	queryArgs.Set("taskname", taskName)

	client := instance.odpsIns.restClient
	var body []byte

	err := client.GetWithParseFunc(instance.resourceUrl, queryArgs, func(res *http.Response) error {
		var err error
		body, err = ioutil.ReadAll(res.Body)
		return errors.WithStack(err)
	})

	if err != nil {
		return "", errors.WithStack(err)
	}

	return string(body), nil
}

// GetCachedInfo 获取instance cached信息，返回的是json字符串，需要自己进行解析
func (instance *Instance) GetCachedInfo() (string, error) {
	queryArgs := make(url.Values, 2)
	queryArgs.Set("cached", "")

	client := instance.odpsIns.restClient
	var body []byte

	err := client.GetWithParseFunc(instance.resourceUrl, queryArgs, func(res *http.Response) error {
		var err error
		body, err = ioutil.ReadAll(res.Body)
		return errors.WithStack(err)
	})

	if err != nil {
		return "", errors.WithStack(err)
	}

	return string(body), nil
}

func (instance *Instance) Id() string {
	return instance.id
}

func (instance *Instance) Owner() string {
	return instance.owner
}

func (instance *Instance) Status() InstanceStatus {
	return instance.status
}

func (instance *Instance) StartTime() time.Time {
	return instance.startTime
}

func (instance *Instance) EndTime() time.Time {
	return instance.endTime
}

func (instance *Instance) TaskResults() []TaskResult {
	return instance.taskResults
}

func (instance *Instance) WaitForSuccess() error {
	for {
		err := instance.Load()
		if err != nil {
			return errors.WithStack(err)
		}

		tasks, err := instance.GetTasks()
		if err != nil {
			return errors.WithStack(err)
		}

		if len(tasks) == 0 {
			time.Sleep(time.Second * 1)
			continue
		}

		success := true

		for _, task := range tasks {
			switch task.Status {
			case TaskFailed, TaskCancelled, TaskSuspended:
				results, err := instance.GetResult()
				if err != nil {
					return errors.Wrapf(err, "get task %s with status %s", task.Name, task.Status)
				}

				if len(results) <= 0 {
					return errors.Errorf("get task %s with status %s", task.Name, task.Status)
				}

				return errors.New(results[0].Content())
			case TaskSuccess:
				continue
			case TaskRunning, TaskWaiting:
				success = false
			}
		}

		if success {
			break
		}

		time.Sleep(time.Second * 1)
	}

	return nil
}

func (instance *Instance) GetResult() ([]TaskResult, error) {
	queryArgs := make(url.Values, 1)
	queryArgs.Set("result", "")
	client := instance.odpsIns.restClient

	type ResModel struct {
		XMLName xml.Name     `xml:"Instance"`
		Tasks   []TaskResult `xml:"Tasks>Task"`
	}

	var resModel ResModel
	err := client.GetWithModel(instance.resourceUrl, queryArgs, &resModel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resModel.Tasks, nil
}

func InstancesStatusFromStr(s string) InstanceStatus {
	switch strings.ToLower(s) {
	case "running":
		return InstanceRunning
	case "suspended":
		return InstanceSuspended
	case "terminated":
		return InstanceTerminated
	default:
		return InstanceStatusUnknown
	}
}

func (status InstanceStatus) String() string {
	switch status {
	case InstanceRunning:
		return "Running"
	case InstanceSuspended:
		return "Suspended"
	case InstanceTerminated:
		return "Terminated"
	default:
		return "InstanceStatusUnknown"
	}
}

func (status *InstanceStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	if err := d.DecodeElement(&s, &start); err != nil {
		return errors.WithStack(err)
	}

	*status = InstancesStatusFromStr(s)

	return nil
}

func (status *InstanceStatus) MarshalXML(d *xml.Encoder, start xml.StartElement) error {
	s := status.String()
	return errors.WithStack(d.EncodeElement(s, start))
}
