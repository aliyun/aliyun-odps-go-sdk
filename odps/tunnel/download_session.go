package tunnel

import (
	"encoding/json"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	restclient2 "github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type DownLoadStatus int

const (
	_ DownLoadStatus = iota
	DownloadStatusUnknown
	DownloadStatusNormal
	DownloadStatusClosed
	DownloadStatusExpired
	DownloadStatusInitiating
)

// DownloadSession 表示从ODPS表中下载数据的会话，
// 需要通过TableTunnel来创建。Session ID是Session的唯一标识符，可通过getId()
// 获取。表中Record总数可通过RecordCount()得到，用户可根据Record总数来启动并发
// 下载。 DownloadSession通过创建RecordReader来完成数据的读取,需指定读取记录的
// 起始位置和数量。目前仅支持{@link RecordArrowReader}。
// RecordReader对应HTTP请求的超时时间为 300S，超时后 service 端会主动关闭。
type DownloadSession struct {
	Id          string
	ProjectName string
	// 暂时没有用到
	SchemaName string
	TableName  string
	// 由于session中用到的partitionKey中不能有"'"。 如, 正确的例子region=hangzhou
	// 错误的例子,region='hangzhou', 用户习惯用后者。为了避免错误，将partitionKey私有
	// 如果用户需要单独设置partitionKey, 则需要使用SetPartitionKey
	partitionKey        string
	UseArrow            bool
	Async               bool
	ShardId             int
	Compressor          Compressor
	RestClient          restclient2.RestClient
	schema              tableschema.TableSchema
	status              DownLoadStatus
	recordCount         int
	shouldTransformDate bool
	arrowSchema         *arrow.Schema
}

// CreateDownloadSession
// Option是可选项，有
// ${@link SessionCfg}.WithPartitionKey(string)
//     下载数据表的partition描述，格式如下: pt=xxx,dt=xxx
// ${@link SessionCfg}.WithShardId(string)
//     下载数据表的shard标识
// ${@link SessionCfg}.Async()
//     异步创建session,小文件多的场景下可以避免连接超时的问题, 默认为false
func CreateDownloadSession(
	projectName, tableName string,
	restClient restclient2.RestClient,
	opts ...Option,
) (*DownloadSession, error) {

	cfg := newSessionConfig(opts...)

	session := DownloadSession{
		ProjectName:  projectName,
		SchemaName:   cfg.SchemaName,
		TableName:    tableName,
		RestClient:   restClient,
		partitionKey: cfg.PartitionKey,
		ShardId:      cfg.ShardId,
		UseArrow:     cfg.UseArrow,
		Async:        cfg.Async,
		Compressor:   cfg.Compressor,
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

func AttachToExistedDownloadSession(
	sessionId, projectName, tableName string,
	restClient restclient2.RestClient,
	opts ...Option,
) (*DownloadSession, error) {

	cfg := newSessionConfig(opts...)

	session := DownloadSession{
		Id:           sessionId,
		ProjectName:  projectName,
		SchemaName:   cfg.SchemaName,
		TableName:    tableName,
		RestClient:   restClient,
		partitionKey: cfg.PartitionKey,
		ShardId:      cfg.ShardId,
		UseArrow:     cfg.UseArrow,
		Async:        cfg.Async,
		Compressor:   cfg.Compressor,
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

func (ds *DownloadSession) Schema() tableschema.TableSchema {
	return ds.schema
}

func (ds *DownloadSession) Status() DownLoadStatus {
	return ds.status
}

func (ds *DownloadSession) RecordCount() int {
	return ds.recordCount
}

func (ds *DownloadSession) ShouldTransformDate() bool {
	return ds.shouldTransformDate
}

func (ds *DownloadSession) ArrowSchema() *arrow.Schema {
	if ds.arrowSchema != nil {
		return ds.arrowSchema
	}
	ds.arrowSchema = ds.schema.ToArrowSchema()
	return ds.arrowSchema
}

func (ds *DownloadSession) PartitionKey() string {
	return ds.partitionKey
}

func (ds *DownloadSession) SetPartitionKey(partitionKey string) {
	ds.partitionKey = strings.ReplaceAll(partitionKey, "'", "")
}

func (ds *DownloadSession) ResourceUrl() string {
	rb := common.NewResourceBuilder(ds.ProjectName)
	return rb.Table(ds.TableName)
}

func (ds *DownloadSession) OpenRecordReader(start, count int, columnNames []string) (*RecordArrowReader, error) {
	arrowSchema := ds.arrowSchema
	if len(columnNames) > 0 {
		arrowFields := make([]arrow.Field, 0, len(columnNames))
		for _, columnName := range columnNames {
			fs, ok := ds.arrowSchema.FieldsByName(columnName)
			if !ok {
				return nil, errors.Errorf("no column names %s in table %s", columnName, ds.TableName)
			}

			arrowFields = append(arrowFields, fs...)
		}

		arrowSchema = arrow.NewSchema(arrowFields, nil)
	}

	res, err := ds.newDownloadConnection(start, count, columnNames)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	reader := newRecordArrowReader(res, arrowSchema)
	return &reader, nil
}

func (ds *DownloadSession) newInitiationRequest() (*http.Request, error) {
	resource := ds.ResourceUrl()
	queryArgs := make(url.Values, 4)
	queryArgs.Set("downloads", "")

	if ds.Async {
		queryArgs.Set("asyncmode", "true")
	}

	if ds.partitionKey != "" {
		queryArgs.Set("partition", ds.partitionKey)
	}

	if ds.ShardId != 0 {
		queryArgs.Set("shard", strconv.Itoa(ds.ShardId))
	}

	req, err := ds.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PostMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (ds *DownloadSession) newLoadRequest() (*http.Request, error) {
	resource := ds.ResourceUrl()
	queryArgs := make(url.Values, 2)
	queryArgs.Set("downloadid", ds.Id)

	if ds.partitionKey != "" {
		queryArgs.Set("partition", ds.partitionKey)
	}

	if ds.ShardId != 0 {
		queryArgs.Set("shard", strconv.Itoa(ds.ShardId))
	}
	req, err := ds.RestClient.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (ds *DownloadSession) loadInformation(req *http.Request) error {
	type ResModel struct {
		DownloadID  string         `json:"DownloadID"`
		Initiated   string         `json:"Initiated"`
		Owner       string         `json:"Owner"`
		RecordCount int            `json:"RecordCount"`
		Schema      schemaResModel `json:"Schema"`
		Status      string         `json:"Status"`
	}

	var resModel ResModel
	err := ds.RestClient.DoWithParseFunc(req, func(res *http.Response) error {
		if res.StatusCode/100 != 2 {
			return restclient2.NewHttpNotOk(res)
		}

		ds.shouldTransformDate = res.Header.Get(common.HttpHeaderOdpsDateTransFrom) == "true"
		decoder := json.NewDecoder(res.Body)
		return decoder.Decode(&resModel)
	})

	if err != nil {
		return errors.WithStack(err)
	}

	tableSchema, err := resModel.Schema.toTableSchema(ds.TableName)
	if err != nil {
		return errors.WithStack(err)
	}

	ds.Id = resModel.DownloadID
	ds.status = DownloadStatusFromStr(resModel.Status)
	ds.recordCount = resModel.RecordCount
	ds.schema = tableSchema
	ds.arrowSchema = tableSchema.ToArrowSchema()

	return nil
}

func (ds *DownloadSession) newDownloadConnection(start, count int, columnNames []string) (*http.Response, error) {
	queryArgs := make(url.Values, 6)

	if len(columnNames) > 0 {
		queryArgs.Set("columns", strings.Join(columnNames, ","))
	}

	queryArgs.Set("downloadid", ds.Id)
	queryArgs.Set("data", "")
	queryArgs.Set("rowrange", fmt.Sprintf("(%d,%d)", start, count))

	if ds.partitionKey != "" {
		queryArgs.Set("partition", ds.partitionKey)
	}

	if ds.UseArrow {
		queryArgs.Set("arrow", "")
	}

	req, err := ds.RestClient.NewRequestWithUrlQuery(
		common.HttpMethod.GetMethod,
		ds.ResourceUrl(),
		nil,
		queryArgs,
	)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	if ds.Compressor != nil {
		req.Header.Set("Accept-Encoding", ds.Compressor.Name())
	}

	addCommonSessionHttpHeader(req.Header)

	res, err := ds.RestClient.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if res.StatusCode/100 != 2 {
		return res, restclient2.NewHttpNotOk(res)
	}

	contentEncoding := res.Header.Get("Content-Encoding")
	if contentEncoding != "" {
		res.Body = WrapByCompressor(res.Body, contentEncoding)
	}

	return res, nil
}

func DownloadStatusFromStr(s string) DownLoadStatus {
	switch strings.ToUpper(s) {
	case "UNKNOWN":
		return DownloadStatusUnknown
	case "NORMAL":
		return DownloadStatusNormal
	case "CLOSED":
		return DownloadStatusClosed
	case "EXPIRED":
		return DownloadStatusExpired
	case "INITIATING":
		return DownloadStatusInitiating
	default:
		return DownloadStatusUnknown
	}
}

func (status DownLoadStatus) String() string {
	switch status {
	case DownloadStatusUnknown:
		return "UNKNOWN"
	case DownloadStatusNormal:
		return "NORMAL"
	case DownloadStatusClosed:
		return "CLOSED"
	case DownloadStatusExpired:
		return "EXPIRED"
	case DownloadStatusInitiating:
		return "INITIATING"
	default:
		return "UNKNOWN"
	}
}
