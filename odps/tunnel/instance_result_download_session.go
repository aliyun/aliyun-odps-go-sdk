package tunnel

import (
	"encoding/json"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type InstanceResultDownloadSession struct {
	Id                  string
	InstanceId          string
	ProjectName         string
	TaskName            string
	QueryId             int
	LimitEnabled        bool
	IsLongPolling       bool
	Compressor          Compressor
	RestClient          restclient.RestClient
	schema              tableschema.TableSchema
	status              DownLoadStatus
	recordCount         int
	shouldTransformDate bool
}

func CreateInstanceResultDownloadSession(
	projectName, instanceId string,
	restClient restclient.RestClient,
	opts ...InstanceOption,
) (*InstanceResultDownloadSession, error) {
	cfg := newInstanceSessionConfig(opts...)

	session := InstanceResultDownloadSession{
		InstanceId:   instanceId,
		ProjectName:  projectName,
		RestClient:   restClient,
		TaskName:     cfg.TaskName,
		QueryId:      cfg.QueryId,
		LimitEnabled: cfg.LimitEnabled,
		Compressor:   cfg.Compressor,
	}

	if cfg.QueryId != -1 {
		session.IsLongPolling = true
	}

	req, err := session.newInitiationRequest()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = session.loadInformation(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &session, nil
}

func AttachToExistedIRDownloadSession(
	downloadId, projectName, instanceId string,
	restClient restclient.RestClient,
	opts ...InstanceOption,
) (*InstanceResultDownloadSession, error) {
	cfg := newInstanceSessionConfig(opts...)

	session := InstanceResultDownloadSession{
		Id:           downloadId,
		InstanceId:   instanceId,
		ProjectName:  projectName,
		RestClient:   restClient,
		TaskName:     cfg.TaskName,
		QueryId:      cfg.QueryId,
		LimitEnabled: cfg.LimitEnabled,
		Compressor:   cfg.Compressor,
	}

	if cfg.QueryId != -1 {
		session.IsLongPolling = true
	}

	req, err := session.newLoadRequest()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = session.loadInformation(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &session, nil
}

func (is *InstanceResultDownloadSession) Schema() tableschema.TableSchema {
	return is.schema
}

func (is *InstanceResultDownloadSession) Status() DownLoadStatus {
	return is.status
}

func (is *InstanceResultDownloadSession) RecordCount() int {
	return is.recordCount
}

func (is *InstanceResultDownloadSession) ShouldTransformDate() bool {
	return is.shouldTransformDate
}

func (is *InstanceResultDownloadSession) ResourceUrl() string {
	rb := common.NewResourceBuilder(is.ProjectName)
	return rb.Instance(is.InstanceId)
}

func (is *InstanceResultDownloadSession) OpenRecordReader(
	start, count, sizeLimit int,
	columnNames []string,
) (*RecordProtocReader, error) {

	var columns []tableschema.Column
	if len(columnNames) > 0 {
		columns = make([]tableschema.Column, len(columnNames))
		for i, columnName := range columnNames {
			c, ok := is.schema.FieldByName(columnName)
			if !ok {
				return nil, errors.Errorf("no column names %s in table", columnName)
			}

			columns[i] = c
		}
	} else {
		columns = is.schema.Columns
	}

	res, err := is.newDownloadConnection(start, count, sizeLimit, columnNames)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	reader := newRecordProtocReader(res, columns, is.shouldTransformDate)
	return &reader, nil
}

func (is *InstanceResultDownloadSession) newInitiationRequest() (*http.Request, error) {
	resource := is.ResourceUrl()
	queryArgs := make(url.Values, 5)
	queryArgs.Set("downloads", "")

	if is.LimitEnabled {
		queryArgs.Set("instance_tunnel_limit_enabled", "")
	}

	if is.TaskName != "" {
		queryArgs.Set("cached", "")
		queryArgs.Set("taskname", "")

		if is.QueryId != -1 {
			queryArgs.Set("queryid", strconv.Itoa(is.QueryId))
		}
	}

	req, err := is.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PostMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (is *InstanceResultDownloadSession) newLoadRequest() (*http.Request, error) {
	resource := is.ResourceUrl()
	queryArgs := make(url.Values, 1)
	queryArgs.Set("downloadid", is.Id)

	req, err := is.RestClient.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (is *InstanceResultDownloadSession) loadInformation(req *http.Request) error {
	type ResModel struct {
		DownloadID  string         `json:"DownloadID"`
		Initiated   string         `json:"Initiated"`
		Owner       string         `json:"Owner"`
		RecordCount int            `json:"RecordCount"`
		Schema      schemaResModel `json:"Schema"`
		Status      string         `json:"Status"`
	}

	var resModel ResModel
	err := is.RestClient.DoWithParseFunc(req, func(res *http.Response) error {
		if res.StatusCode/100 != 2 {
			return errors.WithStack(restclient.NewHttpNotOk(res))
		}

		is.shouldTransformDate = res.Header.Get(common.HttpHeaderOdpsDateTransFrom) == "true"
		decoder := json.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&resModel))
	})

	if err != nil {
		return errors.WithStack(err)
	}

	tableSchema, err := resModel.Schema.toTableSchema("")
	if err != nil {
		return errors.WithStack(err)
	}

	is.Id = resModel.DownloadID
	is.status = DownloadStatusFromStr(resModel.Status)
	is.recordCount = resModel.RecordCount
	is.schema = tableSchema

	return nil
}

func (is *InstanceResultDownloadSession) newDownloadConnection(
	start, count, sizeLimit int,
	columnNames []string,
) (*http.Response, error) {
	queryArgs := make(url.Values, 6)

	if len(columnNames) > 0 {
		queryArgs.Set("columns", strings.Join(columnNames, ","))
	}

	if is.LimitEnabled {
		queryArgs.Set("instance_tunnel_limit_enabled", "")
	}

	queryArgs.Set("data", "")
	if is.IsLongPolling {
		queryArgs.Set("cached", "")
		queryArgs.Set("taskname", "")

		if is.QueryId != -1 {
			queryArgs.Set("queryid", strconv.Itoa(is.QueryId))
		}

		if sizeLimit > 0 {
			queryArgs.Set("sizelimit", strconv.Itoa(sizeLimit))
		}
	} else {
		queryArgs.Set("downloadid", is.Id)
	}

	queryArgs.Set("rowrange", fmt.Sprintf("(%d,%d)", start, count))

	req, err := is.RestClient.NewRequestWithUrlQuery(
		common.HttpMethod.GetMethod,
		is.ResourceUrl(),
		nil,
		queryArgs,
	)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	if is.Compressor != nil {
		req.Header.Set("Accept-Encoding", is.Compressor.Name())
	}

	addCommonSessionHttpHeader(req.Header)

	var res *http.Response

	Retry(func() error {
		res, err = is.RestClient.Do(req)
		return errors.WithStack(err)
	})

	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode/100 != 2 {
		return res, errors.WithStack(restclient.NewHttpNotOk(res))
	}

	contentEncoding := res.Header.Get("Content-Encoding")
	if contentEncoding != "" {
		res.Body = WrapByCompressor(res.Body, contentEncoding)
	}

	return res, nil
}
