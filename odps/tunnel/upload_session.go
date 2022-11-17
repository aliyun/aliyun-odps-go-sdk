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

package tunnel

import (
	"encoding/json"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
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

// UploadSession works as "insert into", multiply sessions for the same table or partition do not affect each other.
// Session id is the unique identifier of a session。
//
// UploadSession uses OpenRecordArrowWriter to create a RecordArrowWriter or OpenRecordWriter to create a RecordProtocWriter
// for writing data into a table. Each RecordWriter uses a http connection to transfer data with the tunnel server, and
// each UploadSession can create multiply RecordWriters, so multiply http connections can be used to upload data
// in parallel.
//
// A block id must be given when creating a RecordWriter, it is the unique identifier of a writer. The block id can be one
// number in [0, 20000)。A single RecordWriter can write at most 100G data。If multiply RecordWriters are created with the
// same block id, the data will be overwritten, and only the data from the writer who calls Close lastly will be kept.
//
// The timeout of http connection used by RecordWriter is 120s， the sever will closeRes the connection when no data occurs in the
// connection during 120 seconds.
//
// The Commit method must be called to notify the server that all data has been upload and the data can be written into
// the table
//
// In particular, the partition keys used by a session can not contain "'", for example, "region=hangzhou" is a
// positive case, and "region='hangzhou'" is a negative case. But the partition keys like "region='hangzhou'" are more
// common, to avoid the users use the error format, the partitionKey of UploadSession is private, it can be set when
// creating a session or using SetPartitionKey.
type UploadSession struct {
	Id          string
	ProjectName string
	// TODO use schema to get the resource url of a table
	SchemaName string
	TableName  string
	// The partition keys used by a session can not contain "'", for example, "region=hangzhou" is a
	// positive case, and "region='hangzhou'" is a negative case. But the partition keys like "region='hangzhou'" are more
	// common, to avoid the users use the error format, the partitionKey of UploadSession is private, it can be set when
	// creating a session or using SetPartitionKey.
	partitionKey        string
	Overwrite           bool
	Compressor          Compressor
	RestClient          restclient.RestClient
	fieldMaxSize        int
	shouldTransformDate bool
	schema              tableschema.TableSchema
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
	u.partitionKey = strings.ReplaceAll(u.partitionKey, " ", "")
}

// CreateUploadSession create a new upload session before uploading data。
// The opts can be one or more of:
// SessionCfg.WithPartitionKey
// SessionCfg.WithSchemaName, it doesn't work now
// SessionCfg.WithDefaultDeflateCompressor, using deflate compressor with default level
// SessionCfg.WithDeflateCompressor, using deflate compressor with specific level
// SessionCfg.WithSnappyFramedCompressor
// SessionCfg.Overwrite, overwrite data
func CreateUploadSession(
	projectName, tableName string,
	restClient restclient.RestClient,
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

// AttachToExistedUploadSession get an existed session by the session id.
// The opts can be one or more of:
// SessionCfg.WithPartitionKey
// SessionCfg.WithSchemaName, it doesn't work now
// SessionCfg.WithDefaultDeflateCompressor, using deflate compressor with default level
// SessionCfg.WithDeflateCompressor, using deflate compressor with specific level
// SessionCfg.WithSnappyFramedCompressor
// SessionCfg.Overwrite, overwrite data
// SessionCfg.UseArrow, it is the default config
func AttachToExistedUploadSession(
	sessionId, projectName, tableName string,
	restClient restclient.RestClient,
	opts ...Option) (*UploadSession, error) {

	cfg := newSessionConfig(opts...)

	session := UploadSession{
		Id:           sessionId,
		ProjectName:  projectName,
		TableName:    tableName,
		partitionKey: cfg.PartitionKey,
		RestClient:   restClient,
		Overwrite:    cfg.Overwrite,
		Compressor:   cfg.Compressor,
	}

	err := session.Load()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &session, nil
}

func (u *UploadSession) Schema() tableschema.TableSchema {
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
	rb := common.NewResourceBuilder(u.ProjectName)
	return rb.Table(u.TableName)
}

func (u *UploadSession) OpenRecordArrowWriter(blockId int) (*RecordArrowWriter, error) {
	conn, err := u.newUploadConnection(blockId, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	writer := newRecordArrowWriter(conn, u.arrowSchema)
	return &writer, nil
}

func (u *UploadSession) OpenRecordWriter(blockId int) (*RecordProtocWriter, error) {
	conn, err := u.newUploadConnection(blockId, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	writer := newRecordProtocHttpWriter(conn, u.schema.Columns, false)
	return &writer, nil
}

func (u *UploadSession) Load() error {
	req, err := u.newLoadRequest()
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(u.loadInformation(req))
}

func (u *UploadSession) Commit(blockIds []int) error {
	err := u.Load()
	if err != nil {
		return errors.WithStack(err)
	}

	errMsgFmt := "blocks server got are %v, blocks uploaded are %v"
	if len(u.blockIds) != len(blockIds) {
		return errors.Errorf(errMsgFmt, u.blockIds, blockIds)
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
			return errors.Errorf(errMsgFmt, u.blockIds, blockIds)
		}
	}

	queryArgs := make(url.Values, 2)
	queryArgs.Set("uploadid", u.Id)
	if u.partitionKey != "" {
		queryArgs.Set("partition", u.partitionKey)
	}

	req, err := u.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PostMethod, u.ResourceUrl(), nil, queryArgs)
	if err != nil {
		return errors.WithStack(err)
	}

	Retry(func() error {
		res, err := u.RestClient.Do(req)
		if err == nil {
			_ = res.Body.Close()
		}
		return errors.WithStack(err)
	})

	return errors.WithStack(err)
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
			return errors.WithStack(restclient.NewHttpNotOk(res))
		}

		u.shouldTransformDate = res.Header.Get(common.HttpHeaderOdpsDateTransFrom) == "true"

		decoder := json.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&resModel))
	})

	if err != nil {
		return errors.WithStack(err)
	}

	tableSchema, err := resModel.Schema.toTableSchema(u.TableName)
	if err != nil {
		return errors.WithStack(err)
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

	req, err := u.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PostMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
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

	req, err := u.RestClient.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
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
	req, err := u.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PutMethod, resource, reader, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	req.Header.Set(common.HttpHeaderContentType, "application/octet-stream")
	addCommonSessionHttpHeader(req.Header)

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
