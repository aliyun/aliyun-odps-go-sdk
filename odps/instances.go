package odps

import (
	"encoding/xml"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Instances is used to get or create instance(s)
type Instances struct {
	projectName string
	odpsIns     *Odps
}

// NewInstances create Instances object, if the projectName is not set,
// the default project name of odpsIns will be used
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
	i, err := instances.CreateTaskWithPriority(projectName, task, DefaultJobPriority)
	return i, errors.WithStack(err)
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
	rb := common.ResourceBuilder{}
	rb.SetProject(projectName)
	resource := rb.Instances()
	var instanceId string
	var isSync bool

	err := client.DoXmlWithParseFunc(common.HttpMethod.PostMethod, resource, nil, &instanceCreationModel, func(res *http.Response) error {
		location := res.Header.Get(common.HttpHeaderLocation)

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
		return errors.WithStack(decoder.Decode(&resModel))
	})

	if err != nil {
		return nil, errors.WithStack(err)
	}

	instance := NewInstance(instances.odpsIns, projectName, instanceId)
	instance.taskNameCommitted = task.GetName()
	instance.taskResults = resModel.Tasks
	instance.isSync = isSync

	return &instance, nil
}

// List Get all instances, the filters can be given with InstanceFilter.Status, InstanceFilter.OnlyOwner,
// InstanceFilter.QuotaIndex, InstanceFilter.TimeRange
func (instances Instances) List(filters ...InsFilterFunc) <-chan InstanceOrErr {
	c := make(chan InstanceOrErr)

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

	go func() {
		defer close(c)
		var resModel ResModel

		for {
			err := client.GetWithModel(resources, queryArgs, &resModel)

			if err != nil {
				c <- InstanceOrErr{nil, errors.WithStack(err)}
				break
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

				c <- InstanceOrErr{&instance, nil}
			}

			if resModel.Marker != "" {
				queryArgs.Set("marker", resModel.Marker)
				resModel = ResModel{}
			} else {
				break
			}
		}
	}()

	return c
}

// ListInstancesQueued Get all instance Queued information, the information is in json string，you need parse it yourself。
// The filters can be given with InstanceFilter.Status, InstanceFilter.OnlyOwner, InstanceFilter.QuotaIndex,
// InstanceFilter.TimeRange
func (instances Instances) ListInstancesQueued(filters ...InsFilterFunc) ([]string, error) {
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
		err := client.GetWithModel(resources, queryArgs, &resModel)

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
