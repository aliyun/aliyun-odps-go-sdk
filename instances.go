package odps

import (
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Instances 表示ODPS中所有Instance的集合
type Instances struct {
	projectName string
	odpsIns     *Odps
}

// NewInstances 如果projectName没有指定，则使用Odps的默认项目名
func NewInstances(odpsIns *Odps, projectName ...string) Instances {
	var _projectName string

	if projectName == nil {
		_projectName = odpsIns.DefaultProjectName()
	} else {
		_projectName = projectName[0]
	}

	return Instances{
		projectName: _projectName,
		odpsIns:     odpsIns,
	}
}

func (instances Instances) CreateTask(projectName string, task Task) (*Instance, error) {
	return instances.CreateTaskWithPriority(projectName, task, DefaultJobPriority)
}

func (instances Instances) CreateTaskWithPriority(projectName string, task Task, jobPriority int) (*Instance, error) {
	uuidStr := uuid.New().String()
	task.AddProperty("uuid", uuidStr)

	type InstanceCreationModel struct {
		XMLName xml.Name `xml:"Instance"`
		Job     struct {
			Priority int
			Tasks    Task `xml:"Tasks>Task"`
		}
	}

	instanceCreationModel := InstanceCreationModel{
		Job: struct {
			Priority int
			Tasks    Task `xml:"Tasks>Task"`
		}{
			Priority: jobPriority,
			Tasks:    task,
		},
	}

	type ResModel struct {
		XMLName xml.Name     `xml:"Instance"`
		Tasks   []TaskResult `xml:"Tasks>Task"`
	}
	var resModel ResModel

	client := instances.odpsIns.restClient
	rb := ResourceBuilder{}
	rb.SetProject(projectName)
	resource := rb.Instances()
	var instanceId string
	var isSync bool

	err := client.DoXmlWithParseFunc(HttpMethod.PostMethod, resource, nil, &instanceCreationModel, func(res *http.Response) error {
		location := res.Header.Get(HttpHeaderLocation)

		if location == "" {
			return errors.New("invalid response, Location header required")
		}

		splitAt := strings.LastIndex(location, "/")
		if splitAt < 0 || splitAt == len(location)-1 {
			return errors.New("invalid response, value of Location header is invalid")
		}

		instanceId = location[splitAt+1:]
		isSync = res.StatusCode == 201

		if isSync {
			return nil
		}

		decoder := xml.NewDecoder(res.Body)
		return decoder.Decode(&resModel)
	})

	if err != nil {
		return nil, err
	}

	instance := NewInstance(instances.odpsIns, projectName, instanceId)
	instance.taskNameCommitted = task.GetName()
	instance.taskResults = resModel.Tasks
	instance.isSync = isSync

	return &instance, nil
}

// List 获取全部的Instance, filter可以忽略或提供一个，提供多个时，只会使用第一个
func (instances Instances) List(c chan Instance, filter ...InstancesFilter) error {
	defer close(c)

	queryArgs := make(url.Values)

	if filter != nil {
		filter[0].fillQueryParams(queryArgs)
	}

	client := instances.odpsIns.restClient
	rb := ResourceBuilder{projectName: instances.projectName}
	resources := rb.Instances()

	type ResModel struct {
		XMLName   xml.Name `xml:"Instances"`
		Marker    string
		MaxItems  int
		Instances []struct {
			Name      string
			Owner     string
			StartTime GMTTime
			EndTime   GMTTime `xml:"EndTime"`
			Status    InstanceStatus
		} `xml:"Instance"`
	}

	var resModel ResModel

	for {
		err := client.GetWithModel(resources, queryArgs, &resModel)

		if err != nil {
			return err
		}

		if len(resModel.Instances) == 0 {
			break
		}

		for _, model := range resModel.Instances {
			instance := NewInstance(instances.odpsIns, instances.projectName, model.Name)
			instance.startTime = time.Time(model.StartTime)
			instance.endTime = time.Time(model.EndTime)
			instance.status = model.Status
			instance.owner = model.Owner

			c <- instance
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

// ListInstancesQueued 获取全部的Instance Queued信息, 信息是json字符串，需要自己进行解析。
// filter可以忽略或提供一个，提供多个时，只会使用第一个
func (instances Instances) ListInstancesQueued(c chan string, filter ...InstancesFilter) error {
	defer close(c)

	queryArgs := make(url.Values)

	if filter != nil {
		filter[0].fillQueryParams(queryArgs)
	}

	client := instances.odpsIns.restClient
	rb := ResourceBuilder{projectName: instances.projectName}
	resources := rb.CachedInstances()

	type ResModel struct {
		XMLName  xml.Name `xml:"Instances"`
		Marker   string
		MaxItems int
		Content  string
	}

	var resModel ResModel

	for {
		err := client.GetWithModel(resources, queryArgs, &resModel)

		if err != nil {
			return err
		}

		if resModel.Content == "" {
			break
		}

		c <- resModel.Content

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return nil
}

type InstancesFilter struct {
	// Instance状态
	Status InstanceStatus
	// 是否只返回提交查询人自己的instance
	OnlyOwner bool
	// Instance 运行所在 quota 组过滤条件
	QuotaIndex string
	// 起始执行时间
	FromTime time.Time
	// 执行结束时间
	EndTime time.Time
}

func (f *InstancesFilter) fillQueryParams(params url.Values) {
	if f.Status != 0 {
		params.Set("status", f.Status.String())
	}

	if f.OnlyOwner {
		params.Set("onlyowner", "yes")
	} else {
		params.Set("onlyowner", "no")
	}

	if f.QuotaIndex != "" {
		params.Set("quotaindex", f.QuotaIndex)
	}

	var startTime string
	var endTime string

	if !f.FromTime.IsZero() {
		startTime = strconv.FormatInt(f.FromTime.Unix(), 10)
	}

	if !f.EndTime.IsZero() {
		endTime = strconv.FormatInt(f.EndTime.Unix(), 10)
	}

	if startTime != "" || endTime != "" {
		dataRange := fmt.Sprintf("%s:%s", startTime, endTime)
		params.Set("datarange", dataRange)
	}
}
