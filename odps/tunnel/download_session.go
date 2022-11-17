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
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
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

// DownloadSession is used to download table data, it can be created by Tunnel.
// You can use RecordCount to get the count of total records, and can create
// multiply RecordReader in parallel according the record count to download
// the data in less time. The RecordArrowReader is the only RecordReader now.
//
// Underneath the RecordReader is the http connection, when no data occurs in it during
// 300s, the tunnel sever will closeRes it.
type DownloadSession struct {
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
	Async               bool
	ShardId             int
	Compressor          Compressor
	RestClient          restclient.RestClient
	schema              tableschema.TableSchema
	status              DownLoadStatus
	recordCount         int
	shouldTransformDate bool
	arrowSchema         *arrow.Schema
}

// CreateDownloadSession create a new download session before downing data.
// The opts can be one or more of:
// SessionCfg.WithPartitionKey
// SessionCfg.WithSchemaName, it doesn't work now
// SessionCfg.WithDefaultDeflateCompressor, using deflate compressor with default level
// SessionCfg.WithDeflateCompressor, using deflate compressor with specific level
// SessionCfg.WithSnappyFramedCompressor
// SessionCfg.Overwrite, overwrite data
// SessionCfg.DisableArrow, disable arrow reader, using protoc reader instead.
// SessionCfg.ShardId, set the shard id of the table
// SessionCfg.Async, enable the async mode of the session which can avoiding timeout when there are many small files
func CreateDownloadSession(
	projectName, tableName string,
	restClient restclient.RestClient,
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

// AttachToExistedDownloadSession get an existed session by the session id.
// The opts can be one or more of:
// SessionCfg.WithPartitionKey
// SessionCfg.WithSchemaName, it doesn't work now
// SessionCfg.WithDefaultDeflateCompressor, using deflate compressor with default level
// SessionCfg.WithDeflateCompressor, using deflate compressor with specific level
// SessionCfg.WithSnappyFramedCompressor
// SessionCfg.Overwrite, overwrite data
// SessionCfg.DisableArrow, disable arrow reader, using protoc reader instead.
// SessionCfg.ShardId, set the shard id of the table
// SessionCfg.Async, enable the async mode of the session which can avoiding timeout when there are many small files
func AttachToExistedDownloadSession(
	sessionId, projectName, tableName string,
	restClient restclient.RestClient,
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
	ds.partitionKey = strings.ReplaceAll(ds.partitionKey, "'", "")
}

func (ds *DownloadSession) ResourceUrl() string {
	rb := common.NewResourceBuilder(ds.ProjectName)
	return rb.Table(ds.TableName)
}

func (ds *DownloadSession) OpenRecordArrowReader(start, count int, columnNames []string) (*RecordArrowReader, error) {
	arrowSchema := ds.arrowSchema
	if len(columnNames) == 0 {
		columnNames = make([]string, len(ds.schema.Columns))
		for i, c := range ds.schema.Columns {
			columnNames[i] = c.Name
		}
	}

	arrowFields := make([]arrow.Field, 0, len(columnNames))
	for _, columnName := range columnNames {
		fs, ok := ds.arrowSchema.FieldsByName(columnName)
		if !ok {
			return nil, errors.Errorf("no column names %s in table %s", columnName, ds.TableName)
		}

		arrowFields = append(arrowFields, fs...)
	}

	arrowSchema = arrow.NewSchema(arrowFields, nil)

	res, err := ds.newDownloadConnection(start, count, columnNames, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	reader := newRecordArrowReader(res, arrowSchema)
	return &reader, nil
}

func (ds *DownloadSession) OpenRecordReader(start, count int, columnNames []string) (*RecordProtocReader, error) {
	if len(columnNames) == 0 {
		columnNames = make([]string, len(ds.schema.Columns))
		for i, c := range ds.schema.Columns {
			columnNames[i] = c.Name
		}
	}

	columns := make([]tableschema.Column, len(columnNames))
	for i, columnName := range columnNames {
		c, ok := ds.schema.FieldByName(columnName)
		if !ok {
			return nil, errors.Errorf("no column names %s in table", columnName)
		}

		columns[i] = c
	}

	res, err := ds.newDownloadConnection(start, count, columnNames, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	reader := newRecordProtocReader(res, columns, ds.shouldTransformDate)
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
			return restclient.NewHttpNotOk(res)
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

func (ds *DownloadSession) newDownloadConnection(start, count int, columnNames []string, useArrow bool) (*http.Response, error) {
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

	if useArrow {
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
		return res, restclient.NewHttpNotOk(res)
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
