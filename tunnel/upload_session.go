package tunnel

import (
	"encoding/json"
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/fetchadd/arrow"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type UploadStatus int

const (
	_ UploadStatus = iota
	UploadStatusUnknown
	UploadStatusNormal
	UploadStatusClosing
	UploadStatusClosed
	UploadStatusCanceled
	UploadStatusExpired
	UploadStatusCritical
	UploadStatusCommitting
)

// UploadSession 向ODPS表上传数据的session
// 上传session是insert into语义，即对同一张 表或partition的多个上传session互不影响。
// session id是session的唯一标志符。
//
// UploadSession通过{@link OpenRecordWriter}方法创建RecordWriter(目前仅支持{@link RecordArrowWriter})来
// 完成写入数据，每个RecordWriter对应一个Http连接, 单个UploadSession可以创建多个RecordArrowWriter
//
// 创建RecordWriter时需指定block ID，block ID是 RecordWriter 的唯一标识符,取值范围 [0, 20000)，单个block上传的数据限制是
// 100G。 同一 UploadSession 中，使用同一block ID多次打开RecordWriter会导致覆盖行为，最后一个调用Close()的RecordWriter
// 所上传的数据会被保留。同一RecordWriter实例不能重复调用Close()
//
// RecordWriter 对应的HTTP连接超时为120s，若120s内没有数据传输，service端会主动关闭连接。特别提醒，HTTP协议本身有8K buffer
//
// 最后调用 {@link Commit} 来提交本次上传的所有数据块。
//
// 由于session中用到的partitionKey中不能有"'"。 如, 正确的例子region=hangzhou
// 错误的例子,region='hangzhou', 用户习惯用后者。为了避免错误，将partitionKey私有
// 如果用户需要单独设置partitionKey, 则需要使用SetPartitionKey
type UploadSession struct {
	Id          string
	ProjectName string
	// 暂时没有用到
	SchemaName string
	TableName  string
	// 由于session中用到的partitionKey中不能有"'"。 如, 正确的例子region=hangzhou
	// 错误的例子,region='hangzhou', 用户习惯用后者。为了避免错误，将partitionKey私有
	// 如果用户需要单独设置partitionKey, 则需要使用SetPartitionKey
	partitionKey        string
	Overwrite           bool
	UseArrow            bool
	Compressor          Compressor
	RestClient          odps.RestClient
	fieldMaxSize        int
	shouldTransformDate bool
	schema              odps.TableSchema
	status              UploadStatus
	arrowSchema         *arrow.Schema
	blockIds            []int
}

func (u *UploadSession) BlockIds() []int {
	return u.blockIds
}

func (u *UploadSession) PartitionKey() string {
	return u.partitionKey
}

func (u *UploadSession) SetPartitionKey(partitionKey string) {
	u.partitionKey = strings.ReplaceAll(partitionKey, "'", "")
}

// CreateUploadSession 创建一个新的UploadSession
func CreateUploadSession(
	projectName, tableName string,
	restClient odps.RestClient,
	opts ...Option,
) (*UploadSession, error) {

	cfg := newSessionConfig(opts...)

	session := UploadSession{
		ProjectName:  projectName,
		SchemaName:   cfg.SchemaName,
		TableName:    tableName,
		partitionKey: cfg.PartitionKey,
		RestClient:   restClient,
		Overwrite:    cfg.Overwrite,
		UseArrow:     cfg.UseArrow,
		Compressor:   cfg.Compressor,
	}

	req, err := session.newInitiationRequest()
	if err != nil {
		return nil, err
	}

	err = session.loadInformation(req)
	if err != nil {
		return nil, err
	}

	return &session, nil
}

// AttachToExistedUploadSession 根据已有的session id获取session
func AttachToExistedUploadSession(
	sessionId, projectName, tableName string,
	restClient odps.RestClient,
	opts ...Option) (*UploadSession, error) {

	cfg := newSessionConfig(opts...)

	session := UploadSession{
		Id:           sessionId,
		ProjectName:  projectName,
		TableName:    tableName,
		partitionKey: cfg.PartitionKey,
		RestClient:   restClient,
		UseArrow:     cfg.UseArrow,
		Overwrite:    cfg.Overwrite,
		Compressor:   cfg.Compressor,
	}

	err := session.Load()
	if err != nil {
		return nil, err
	}

	return &session, nil
}

func (u *UploadSession) Schema() odps.TableSchema {
	return u.schema
}

func (u *UploadSession) ArrowSchema() *arrow.Schema {
	if u.arrowSchema != nil {
		return u.arrowSchema
	}
	u.arrowSchema = u.schema.ToArrowSchema()
	return u.arrowSchema
}

func (u *UploadSession) Status() UploadStatus {
	return u.status
}

func (u *UploadSession) ShouldTransform() bool {
	return u.shouldTransformDate
}

func (u *UploadSession) ResourceUrl() string {
	rb := odps.NewResourceBuilder(u.ProjectName)
	return rb.Table(u.TableName)
}

func (u *UploadSession) OpenRecordWriter(blockId int) (*RecordArrowWriter, error) {
	conn, err := u.newUploadConnection(blockId, u.UseArrow)
	if err != nil {
		return nil, err
	}

	writer := newRecordArrowWriter(conn, u.arrowSchema)
	return &writer, nil
}

func (u *UploadSession) Load() error {
	req, err := u.newLoadRequest()
	if err != nil {
		return err
	}

	return u.loadInformation(req)
}

func (u *UploadSession) Commit(blockIds []int) error {
	err := u.Load()
	if err != nil {
		return err
	}

	errMsgFmt := "blocks server got are %v, blocks uploaded are %v"
	if len(u.blockIds) != len(blockIds) {
		return fmt.Errorf(errMsgFmt, u.blockIds, blockIds)
	}

	for _, idWanted := range blockIds {
		found := false

		for _, realId := range u.blockIds {
			if idWanted == realId {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf(errMsgFmt, u.blockIds, blockIds)
		}
	}

	queryArgs := make(url.Values, 2)
	queryArgs.Set("uploadid", u.Id)
	if u.partitionKey != "" {
		queryArgs.Set("partition", u.partitionKey)
	}

	req, err := u.RestClient.NewRequestWithUrlQuery(odps.HttpMethod.PostMethod, u.ResourceUrl(), nil, queryArgs)
	if err != nil {
		return err
	}

	Retry(func() error {
		_, err = u.RestClient.Do(req)
		return err
	})

	return err
}

func (u *UploadSession) loadInformation(req *http.Request) error {
	type ResModel struct {
		Initiated         string         `json:"Initiated"`
		IsOverwrite       bool           `json:"IsOverwrite"`
		MaxFieldSize      int            `json:"MaxFieldSize"`
		Owner             string         `json:"Owner"`
		Schema            schemaResModel `json:"Schema"`
		Status            string         `json:"Status"`
		UploadID          string         `json:"UploadID"`
		UploadedBlockList []struct {
			BlockID     int    `json:"BlockID"`
			CreateTime  int    `json:"CreateTime"`
			Date        string `json:"Date"`
			FileName    string `json:"FileName"`
			RecordCount int    `json:"RecordCount"`
			Version     int64  `json:"Version"`
		} `json:"UploadedBlockList"`
	}

	var resModel ResModel
	err := u.RestClient.DoWithParseFunc(req, func(res *http.Response) error {
		if res.StatusCode/100 != 2 {
			return odps.NewHttpNotOk(res)
		}

		u.shouldTransformDate = res.Header.Get(odps.HttpHeaderOdpsDateTransFrom) == "true"

		decoder := json.NewDecoder(res.Body)
		return decoder.Decode(&resModel)
	})

	if err != nil {
		return err
	}

	tableSchema, err := resModel.Schema.toTableSchema(u.TableName)
	if err != nil {
		return err
	}

	u.Id = resModel.UploadID
	u.fieldMaxSize = resModel.MaxFieldSize
	u.status = UploadStatusFromStr(resModel.Status)
	u.schema = tableSchema
	u.arrowSchema = tableSchema.ToArrowSchema()
	u.blockIds = make([]int, len(resModel.UploadedBlockList))
	for i, b := range resModel.UploadedBlockList {
		u.blockIds[i] = b.BlockID
	}

	return nil
}

func (u *UploadSession) newInitiationRequest() (*http.Request, error) {
	resource := u.ResourceUrl()
	queryArgs := make(url.Values, 3)
	queryArgs.Set("uploads", "")

	if u.partitionKey != "" {
		queryArgs.Set("partition", u.partitionKey)
	}

	if u.Overwrite {
		queryArgs.Set("override", "true")
	}

	req, err := u.RestClient.NewRequestWithUrlQuery(odps.HttpMethod.PostMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, err
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (u *UploadSession) newLoadRequest() (*http.Request, error) {
	resource := u.ResourceUrl()
	queryArgs := make(url.Values, 2)
	queryArgs.Set("uploadid", u.Id)

	if u.partitionKey != "" {
		queryArgs.Set("partition", u.partitionKey)
	}

	req, err := u.RestClient.NewRequestWithUrlQuery(odps.HttpMethod.GetMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, err
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (u *UploadSession) newUploadConnection(blockId int, useArrow bool) (*httpConnection, error) {

	queryArgs := make(url.Values, 4)
	queryArgs.Set("uploadid", u.Id)
	queryArgs.Set("blockid", strconv.Itoa(blockId))

	if useArrow {
		queryArgs.Set("arrow", "")
	}

	if u.partitionKey != "" {
		queryArgs.Set("partition", u.partitionKey)
	}

	var reader io.ReadCloser
	var writer io.WriteCloser
	reader, writer = io.Pipe()

	resource := u.ResourceUrl()
	req, err := u.RestClient.NewRequestWithUrlQuery(odps.HttpMethod.PutMethod, resource, reader, queryArgs)
	req.Header.Set(odps.HttpHeaderContentType, "application/octet-stream")
	addCommonSessionHttpHeader(req.Header)
	if err != nil {
		return nil, err
	}

	if u.Compressor != nil {
		req.Header.Set("Content-Encoding", u.Compressor.Name())
		writer = u.Compressor.NewWriter(writer)
	}

	resChan := make(chan resOrErr)

	go func() {
		res, err := u.RestClient.Do(req)
		resChan <- resOrErr{err: err, res: res}
	}()

	return &httpConnection{
		Writer:  writer,
		resChan: resChan,
	}, nil
}

func UploadStatusFromStr(s string) UploadStatus {
	switch strings.ToUpper(s) {
	case "NORMAL":
		return UploadStatusNormal
	case "CLOSING":
		return UploadStatusClosing
	case "CLOSED":
		return UploadStatusClosed
	case "CANCELED":
		return UploadStatusCanceled
	case "EXPIRED":
		return UploadStatusExpired
	case "CRITICAL":
		return UploadStatusCritical
	case "COMMITTING":
		return UploadStatusCommitting
	default:
		return UploadStatusUnknown
	}
}

func (status UploadStatus) String() string {
	switch status {
	case UploadStatusUnknown:
		return "UNKNOWN"
	case UploadStatusNormal:
		return "NORMAL"
	case UploadStatusClosing:
		return "CLOSING"
	case UploadStatusClosed:
		return "CLOSED"
	case UploadStatusCanceled:
		return "CANCELED"
	case UploadStatusExpired:
		return "EXPIRED"
	case UploadStatusCritical:
		return "CRITICAL"
	case UploadStatusCommitting:
		return "COMMITTING"
	default:
		return "UNKNOWN"
	}
}
