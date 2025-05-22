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
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
)

// Instances is used to get or create instance(s)
type Instances struct {
	projectName string
	odpsIns     *Odps
}

// NewInstances create Instances object, if the projectName is not set,
// the default project name of odpsIns will be used
func NewInstances(odpsIns *Odps, projectName ...string) *Instances {
	var _projectName string

	if len(projectName) == 0 {
		_projectName = odpsIns.DefaultProjectName()
	} else {
		_projectName = projectName[0]
	}

	return &Instances{
		projectName: _projectName,
		odpsIns:     odpsIns,
	}
}

// CreateTaskWithPriority Create a Task (maybe SQLTask) with specified priority
func (instances *Instances) CreateTaskWithPriority(projectName string, task Task, jobPriority int) (*Instance, error) {
	instanceOptions := options.NewCreateInstanceOptions()
	instanceOptions.Priority = jobPriority
	return instances.CreateTask(projectName, task, instanceOptions)
}

// CreateTask Create a Task (maybe SQLTask) with options.CreateInstanceOptions
func (instances *Instances) CreateTask(projectName string, task Task, createInstanceOptions ...*options.CreateInstanceOptions) (*Instance, error) {
	var instanceOptions *options.CreateInstanceOptions
	if len(createInstanceOptions) != 0 {
		instanceOptions = createInstanceOptions[0]
	}
	if instanceOptions == nil {
		instanceOptions = options.NewCreateInstanceOptions()
	}
	if projectName == "" {
		projectName = instances.projectName
	}
	jobPriority := instanceOptions.Priority
	if jobPriority == 0 {
		jobPriority = DefaultJobPriority
	}

	uuidStr := uuid.New().String()
	task.AddProperty("uuid", uuidStr)

	// The order of each field is strictly ordered
	type InstanceCreationModel struct {
		XMLName xml.Name `xml:"Instance"`
		Job     struct {
			Name             string `xml:"Name,omitempty"`
			Priority         int
			UniqueIdentifyID string `xml:"Guid,omitempty"`
			Tasks            Task   `xml:"Tasks>Task"`
		}
	}

	instanceCreationModel := InstanceCreationModel{
		Job: struct {
			Name             string `xml:"Name,omitempty"`
			Priority         int
			UniqueIdentifyID string `xml:"Guid,omitempty"`

			Tasks Task `xml:"Tasks>Task"`
		}{
			Name:             instanceOptions.JobName,
			Priority:         jobPriority,
			UniqueIdentifyID: instanceOptions.UniqueIdentifyID,
			Tasks:            task,
		},
	}

	type ResModel struct {
		XMLName xml.Name     `xml:"Instance"`
		Tasks   []TaskResult `xml:"Tasks>Task"`
	}
	var resModel ResModel

	client := instances.odpsIns.restClient
	rb := common.ResourceBuilder{}
	rb.SetProject(projectName)
	resource := rb.Instances()

	queryArg := make(url.Values)
	if instanceOptions.TryWait {
		queryArg.Set("tryWait", "")
	}
	var instanceId string

	headers := make(map[string]string)

	maxqaOptions := instanceOptions.MaxQAOptions
	if maxqaOptions.UseMaxQA {
		if maxqaOptions.SessionID == "" && maxqaOptions.QuotaName == "" {
			return nil, errors.New("The MaxQA job must provide a SessionID or QuotaName.")
		}
		if maxqaOptions.SessionID == "" {
			id, err := getMaxQASessionID(instances.odpsIns, maxqaOptions.QuotaName, projectName)
			if err != nil {
				return nil, err
			}
			maxqaOptions.SessionID = id
		}
		resource = "/mcqa" + resource
		headers[common.HttpHeaderMaxQASessionID] = maxqaOptions.SessionID
	}
	var maxqaQueryCookie string

	startTime := time.Now()
	maxRetryDuration := 180 * time.Second

	// 循环，直到达到最大重试时间
	for {
		err := client.DoXmlWithParseFunc(common.HttpMethod.PostMethod, resource, queryArg, headers, &instanceCreationModel, func(res *http.Response) error {
			location := res.Header.Get(common.HttpHeaderLocation)

			if location == "" {
				return errors.New("invalid response, Location header required")
			}

			splitAt := strings.LastIndex(location, "/")
			if splitAt < 0 || splitAt == len(location)-1 {
				return errors.New("invalid response, value of Location header is invalid")
			}

			instanceId = location[splitAt+1:]
			if res.StatusCode == 409 {
				return restclient.NewHttpNotOk(res)
			}
			if res.StatusCode == 201 {
				maxqaQueryCookie = res.Header.Get(common.HttpHeaderMaxQAQueryCookie)
				return nil
			}
			decoder := xml.NewDecoder(res.Body)
			return errors.WithStack(decoder.Decode(&resModel))
		})
		if err != nil {
			if time.Since(startTime) >= maxRetryDuration {
				return nil, err
			}
			var httpErr restclient.HttpError
			if errors.As(err, &httpErr) && httpErr.Response.StatusCode == 409 {
				retryAfter := httpErr.Response.Header.Get("Retry-After")
				if retryAfter != "" {
					retryAfterInt, ioErr := strconv.Atoi(retryAfter)
					if ioErr != nil {
						retryAfterInt = 5
					}
					time.Sleep(time.Second * time.Duration(retryAfterInt))
				} else {
					time.Sleep(time.Second * 5)
				}
				continue
			}
			return nil, err
		}
		instance := NewInstance(instances.odpsIns, projectName, instanceId)
		instance.taskNameCommitted = task.GetName()
		instance.taskResults = resModel.Tasks
		instance.isSync = resModel.Tasks != nil && len(resModel.Tasks) > 0
		if maxqaOptions.UseMaxQA {
			instance.MaxQA.IsMaxQA = maxqaOptions.UseMaxQA
			instance.MaxQA.SessionID = maxqaOptions.SessionID
			instance.MaxQA.QueryCookie = maxqaQueryCookie
		}
		return instance, nil
	}
}

// List Get all instances, the filters can be given with InstanceFilter.Status, InstanceFilter.OnlyOwner,
// InstanceFilter.QuotaIndex, InstanceFilter.TimeRange
func (instances *Instances) List(f func(*Instance), filters ...InsFilterFunc) error {
	queryArgs := make(url.Values)
	queryArgs.Set("onlyowner", "no")

	for _, filter := range filters {
		filter(queryArgs)
	}

	client := instances.odpsIns.restClient
	rb := common.ResourceBuilder{ProjectName: instances.projectName}
	resources := rb.Instances()

	type ResModel struct {
		XMLName   xml.Name `xml:"Instances"`
		Marker    string
		MaxItems  int
		Instances []struct {
			Name      string
			Owner     string
			StartTime common.GMTTime
			EndTime   common.GMTTime `xml:"EndTime"`
			Status    InstanceStatus
		} `xml:"Instance"`
	}

	var resModel ResModel
	for {
		err := client.GetWithModel(resources, queryArgs, nil, &resModel)
		if err != nil {
			return err
		}

		for _, model := range resModel.Instances {
			instance := NewInstance(instances.odpsIns, instances.projectName, model.Name)
			instance.startTime = time.Time(model.StartTime)
			instance.endTime = time.Time(model.EndTime)
			instance.status = model.Status
			instance.owner = model.Owner

			f(instance)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return nil
}

// ListInstancesQueued Get all instance Queued information, the information is in json string，you need parse it yourself。
// The filters can be given with InstanceFilter.Status, InstanceFilter.OnlyOwner, InstanceFilter.QuotaIndex,
// InstanceFilter.TimeRange
func (instances *Instances) ListInstancesQueued(filters ...InsFilterFunc) ([]string, error) {
	queryArgs := make(url.Values)
	queryArgs.Set("onlyowner", "no")

	for _, filter := range filters {
		filter(queryArgs)
	}

	client := instances.odpsIns.restClient
	rb := common.ResourceBuilder{ProjectName: instances.projectName}
	resources := rb.CachedInstances()

	type ResModel struct {
		XMLName  xml.Name `xml:"Instances"`
		Marker   string
		MaxItems int
		Content  string
	}

	var resModel ResModel
	var insList []string

	for {
		err := client.GetWithModel(resources, queryArgs, nil, &resModel)
		if err != nil {
			return insList, errors.WithStack(err)
		}

		if resModel.Content == "" {
			break
		}

		insList = append(insList, resModel.Content)

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return insList, nil
}

type InsFilterFunc func(values url.Values)

var InstanceFilter = struct {
	// Only get instances with a given status
	Status func(InstanceStatus) InsFilterFunc
	// Only get instances that create by the current account
	OnlyOwner func() InsFilterFunc
	// Instance 运行所在 quota 组过滤条件
	QuotaIndex func(string) InsFilterFunc
	// Get instances running between start and end times
	TimeRange func(time.Time, time.Time) InsFilterFunc
}{
	Status: func(status InstanceStatus) InsFilterFunc {
		return func(values url.Values) {
			if status != 0 {
				values.Set("status", status.String())
			}
		}
	},

	OnlyOwner: func() InsFilterFunc {
		return func(values url.Values) {
			values.Set("onlyowner", "yes")
		}
	},

	QuotaIndex: func(s string) InsFilterFunc {
		return func(values url.Values) {
			values.Set("quotaindex", s)
		}
	},

	TimeRange: func(s time.Time, e time.Time) InsFilterFunc {
		return func(values url.Values) {
			startTime := strconv.FormatInt(s.Unix(), 10)
			endTime := strconv.FormatInt(e.Unix(), 10)

			dateRange := fmt.Sprintf("%s:%s", startTime, endTime)
			values.Set("daterange", dateRange)
		}
	},
}

func (instances *Instances) Get(instanceId string) *Instance {
	return NewInstance(instances.odpsIns, instances.projectName, instanceId)
}

func getMaxQASessionID(ins *Odps, quotaName string, projectName string) (string, error) {
	tenantId := ins.Project(projectName).TenantId()
	resource := "/quotas/" + quotaName
	queryArgs := make(url.Values, 4)
	queryArgs.Set("project", projectName)
	queryArgs.Set("version", "wlm")
	queryArgs.Set("tenant", tenantId)

	request, err := ins.restClient.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
	if err != nil {
		return "", err
	}
	response, err := ins.restClient.Do(request)
	if err != nil {
		return "", err
	}
	maxqaSessionId := response.Header.Get(common.HttpHeaderMaxQASessionID)
	return maxqaSessionId, nil
}
