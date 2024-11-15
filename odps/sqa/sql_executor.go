package sqa

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	tunnel2 "github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	DEFAULT_TASK_NAME = "sqlrt_task"
	DEFAULT_SERVICE   = "public.default"

	SESSION_TIMEOUT = 60

	OBJECT_STATUS_RUNNING    = 2
	OBJECT_STATUS_FAILED     = 4
	OBJECT_STATUS_TERMINATED = 5
	OBJECT_STATUS_CANCELLED  = 6
)

type SQLExecutor interface {
	Close()
	Cancel()
	Run(sql string, hints map[string]string)
	GetResult() ([]data.Record, error)
}

type InteractiveSQLExecutor struct {
	odpsIns        *odps.Odps
	taskName       string
	serviceName    string
	hints          map[string]string // for create instance
	queryHints     map[string]string // for query
	id             string
	sql            string
	runningCluster string
	instance       *odps.Instance
	subQueryInfo   *SubQueryInfo
}

type SQLExecutorQueryParam struct {
	OdpsIns        *odps.Odps
	TaskName       string
	ServiceName    string
	RunningCluster string
	Hints          map[string]string
}

func NewInteractiveSQLExecutor(params *SQLExecutorQueryParam) *InteractiveSQLExecutor {
	id := uuid.New().String()

	hints := params.Hints
	if hints == nil {
		hints = make(map[string]string)
	}

	return &InteractiveSQLExecutor{
		odpsIns:        params.OdpsIns,
		id:             id,
		taskName:       params.TaskName,
		serviceName:    params.ServiceName,
		runningCluster: params.RunningCluster,
		hints:          hints,
	}
}

// Run submit a query to instance
func (ie *InteractiveSQLExecutor) Run(sql string, queryHints map[string]string) error {
	//
	if queryHints == nil {
		queryHints = make(map[string]string)
	}
	// init InteractiveSQLExecutor
	ie.sql = sql
	ie.queryHints = queryHints
	if ie.queryHints == nil {
		ie.queryHints = make(map[string]string)
	}
	//
	var err error
	ie.instance, err = ie.createInstance()
	if err != nil {
		return errors.Wrapf(err, "Get error when creating instance")
	}
	// wait for attach success
	err = ie.waitAttachSuccess(SESSION_TIMEOUT)
	if err != nil {
		return err
	}
	//
	err = ie.runQueryInternal()
	if err != nil {
		return errors.Wrapf(err, "Get error when creating running query: %v", ie.sql)
	}
	return nil
}

func (ie *InteractiveSQLExecutor) createInstance() (*odps.Instance, error) {
	if ie.serviceName != "" {
		ie.hints["odps.sql.session.share.id"] = ie.serviceName
		ie.hints["odps.sql.session.name"] = strings.TrimSpace(ie.serviceName)
	} else {
		return nil, errors.New("service name cannot be empty.")
	}

	if ie.taskName == "" {
		ie.taskName = DEFAULT_TASK_NAME
	}
	//
	projectName := ie.odpsIns.DefaultProjectName()
	// change "odps.sql.submit.mode" flag
	userSubmitMode, ok := ie.hints["odps.sql.submit.mode"]
	ie.hints["odps.sql.submit.mode"] = "script"
	//
	task := odps.NewSqlRTTask(ie.taskName, "", ie.hints)

	if ok {
		ie.hints["odps.sql.submit.mode"] = userSubmitMode
	}
	//
	instances := odps.NewInstances(ie.odpsIns, projectName)
	instance, err := instances.CreateTask(projectName, &task)
	if err != nil {
		return nil, err
	}
	return instance, err
}

type SubQueryResponse struct {
	Status     int
	Result     string
	warnings   string
	SubQueryId int
}

func (ie *InteractiveSQLExecutor) waitAttachSuccess(timeout int64) error {
	if timeout < 1 {
		timeout = SESSION_TIMEOUT
	}
	//
	start := time.Now()
	end := start.Add(time.Second * time.Duration(timeout))
	//
	for time.Now().Before(end) {
		infoStr, err := ie.instance.GetTaskInfo(ie.taskName, "wait_attach_success")
		//
		var subQueryResp SubQueryResponse
		_ = json.Unmarshal([]byte(infoStr), &subQueryResp)
		if err != nil || subQueryResp.Status == 0 {
			// check task status
			tasks, err := ie.instance.GetTasks()
			if err != nil {
				return err
			}
			//
			var status odps.TaskStatus
			for _, task := range tasks {
				if task.Name == ie.taskName {
					status = task.Status
					break
				}
			}
			if status != odps.TaskRunning {
				return errors.New(fmt.Sprintf("instance id: %v, task name: %s, status: %v",
					ie.instance.Id(), ie.taskName, status.String()))
			}
		} else if subQueryResp.Status == OBJECT_STATUS_FAILED || subQueryResp.Status == OBJECT_STATUS_TERMINATED {
			return errors.New(fmt.Sprintf("attach instance [id: %v] failed, %s", ie.instance.Id(), subQueryResp.Result))
		}
		// running
		return nil
	}
	//
	_ = ie.instance.Terminate()
	return errors.New(fmt.Sprintf("attach instance [id: %v] timeout.", ie.instance.Id()))
}

type SubQueryInfo struct {
	QueryId int    `json:"queryId"`
	Status  string `json:"status"`
	Result  string `json:"result"`
}

func (ie *InteractiveSQLExecutor) runQueryInternal() error {
	request := make(map[string]interface{})
	//
	request["query"] = ie.sql
	if ie.hints == nil {
		ie.hints = make(map[string]string)
	}
	request["settings"] = ie.queryHints
	requestJson, _ := json.Marshal(request)
	// instance set information
	res, err := ie.instance.UpdateInfo(ie.taskName, "query", string(requestJson))
	if err != nil {
		return err
	}
	//
	var subQueryInfo SubQueryInfo
	if res.Status != "ok" {
		subQueryInfo.Status = res.Status
		subQueryInfo.Result = res.Result
	} else if res.Result != "" {
		err = json.Unmarshal([]byte(res.Result), &subQueryInfo)
		if err != nil {
			return errors.Wrapf(err, "%+v", res.Result)
		}
	} else {
		return errors.Errorf("Invalid result: %+v", res)
	}
	ie.subQueryInfo = &subQueryInfo
	//
	return nil
}

// GetResult get query result by instance tunnel
func (ie *InteractiveSQLExecutor) GetResult(offset, countLimit, sizeLimit int, limitEnabled bool) ([]data.Record, error) {
	//
	ds, err := ie.GetDownloadSession(limitEnabled)
	if err != nil {
		return nil, err
	}
	//
	reader, err := ds.OpenRecordReader(offset, countLimit, sizeLimit, []string{})
	if err != nil {
		return nil, err
	}
	//
	results := make([]data.Record, 0, countLimit)
	for {
		record, err := reader.Read()
		if err != nil {
			isEOF := errors.Is(err, io.EOF)
			if isEOF {
				break
			}
			return nil, err
		}
		results = append(results, record)
	}
	//
	return results, nil
}

func (ie *InteractiveSQLExecutor) GetDownloadSession(limitEnabled bool) (*tunnel2.InstanceResultDownloadSession, error) {
	if ie.instance == nil {
		return nil, errors.New("InteractiveSQLExecutor.instance is nil, please create instance first")
	}
	//
	projects := ie.odpsIns.Projects()
	project := projects.Get(ie.instance.ProjectName())
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		return nil, err
	}
	tunnel := tunnel2.NewTunnel(ie.odpsIns, tunnelEndpoint)
	//
	opts := make([]tunnel2.InstanceOption, 0)
	opts = append(opts, tunnel2.InstanceSessionCfg.WithTaskName(ie.taskName))
	opts = append(opts, tunnel2.InstanceSessionCfg.WithQueryId(ie.subQueryInfo.QueryId))
	if limitEnabled {
		opts = append(opts, tunnel2.InstanceSessionCfg.EnableLimit())
	}

	//
	return tunnel.CreateInstanceResultDownloadSession(project.Name(), ie.instance.Id(), opts...)
}

func (ie *InteractiveSQLExecutor) Close() error {
	return ie.instance.Terminate()
}

func (ie *InteractiveSQLExecutor) Cancel() error {
	updateInfoResult, err := ie.instance.UpdateInfo(ie.taskName, "cancel", strconv.Itoa(ie.subQueryInfo.QueryId))
	if err != nil {
		return err
	}
	//
	if updateInfoResult.Status != "ok" {
		return errors.New(fmt.Sprintf("cancel failed, message: %s", updateInfoResult.Result))
	}

	return nil
}
