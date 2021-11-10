package odps

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type InstanceStatus int

const (
	_ InstanceStatus = iota
	InstanceRunning
	InstanceSuspended
	InstanceTerminated
	InstanceStatusUnknown
)

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
	tasksGenerated    []string
	isSync            bool
}

func NewInstance(odpsIns *Odps, projectName, instanceId string) Instance {
	rb := ResourceBuilder{projectName: projectName}

	return Instance{
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

func (instance *Instance) HasBeLoaded() bool {
	return instance.reLoaded
}

func (instance *Instance) IsSync() bool  {
	return instance.isSync
}

func (instance *Instance) IsAsync() bool  {
	return ! instance.isSync
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
		instance.owner = header.Get(HttpHeaderOdpsOwner)
		instance.startTime, _ = ParseRFC1123Date(header.Get(HttpHeaderOdpsStartTime))
		instance.endTime, _ = ParseRFC1123Date(header.Get(HttpHeaderOdpsEndTime))

		decoder := xml.NewDecoder(res.Body)
		if err := decoder.Decode(&resModel); err != nil {
			return err
		}

		instance.status = resModel.Status
		return nil
	})

	return err
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
	return client.DoXmlWithParseFunc("PUT", instance.resourceUrl, nil, &bodyModel, nil)
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
		return nil, err
	}

	instance.tasksGenerated = make([]string, len(resModel.Tasks))
	for i, task := range resModel.Tasks {
		instance.tasksGenerated[i] = task.Name
	}

	return resModel.Tasks, nil
}

func (instance *Instance) GetTaskProgress(taskName string) ([]TaskProgressStage, error) {
	nameIsOk := false

	for _, _taskName := range instance.tasksGenerated {
		if _taskName == taskName {
			nameIsOk = true
			break
		}
	}

	if !nameIsOk {
		return nil, errors.New(fmt.Sprintf("task %s is not belong to instance %s", taskName, instance.id))
	}

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
		return nil, err
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
		return err
	})

	return body, err
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
		return decoder.Decode(&resModel)
	})

	if err != nil {
		return nil, err
	}

	taskSummary := TaskSummary{
		JsonSummary: resModel.Instance.JsonSummary,
		Summary:     resModel.Instance.Summary,
	}

	return &taskSummary, err
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
		return err
	})

	if err != nil {
		return "", err
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
		return err
	})

	if err != nil {
		return "", err
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

func (instance *Instance) WaitForSuccess() error {
	for  {
		err := instance.Load()
		if err != nil {
			return err
		}

		tasks, err := instance.GetTasks()
		if err != nil {
			return err
		}

		success := true

		for _, task := range tasks {
			switch task.Status {
			case TaskFailed, TaskCancelled, TaskSuspended:
				return errors.New(fmt.Sprintf("get task %s with status %s", task.Name, task.Status))
			case TaskSuccess:
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

func (status *InstanceStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	*status = InstancesStatusFromStr(s)

	return nil
}

func (status *InstanceStatus) MarshalXML(d *xml.Encoder, start xml.StartElement) error {
	s := status.String()
	return d.EncodeElement(s, start)
}
