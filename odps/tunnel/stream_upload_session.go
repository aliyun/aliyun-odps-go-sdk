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
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

type StreamUploadSession struct {
	id          string
	ProjectName string
	// TODO use schema to get the resource url of a table
	SchemaName string
	TableName  string
	// The partition keys used by a session can not contain "'", for example, "region=hangzhou" is a
	// positive case, and "region='hangzhou'" is a negative case. But the partition keys like "region='hangzhou'" are more
	// common, to avoid the users use the error format, the partitionKey of UploadSession is private, it can be set when
	// creating a session or using SetPartitionKey.
	partitionKey        string
	Compressor          Compressor
	RestClient          restclient.RestClient
	Columns             []string
	P2PMode             bool
	CreatePartition     bool
	QuotaName           string
	SlotNum             int
	slotSelector        slotSelector
	schema              tableschema.TableSchema
	schemaVersion       int
	allowSchemaMismatch bool
}

func (su *StreamUploadSession) ResourceUrl() string {
	rb := common.NewResourceBuilder(su.ProjectName)
	tableResource := rb.Table(su.SchemaName, su.TableName)
	return tableResource + common.StreamsPath
}

// CreateStreamUploadSession create a new stream upload session before uploading data。
// The opts can be one or more of:
// SessionCfg.WithPartitionKey
// SessionCfg.WithSchemaName
// SessionCfg.WithDefaultDeflateCompressor, using deflate compressor with default level
// SessionCfg.WithDeflateCompressor, using deflate compressor with specific level
// SessionCfg.WithSnappyFramedCompressor
// SessionCfg.SlotNum, 暂不对外开放
// SessionCfg.CreatePartition, create partition if the partition specified by WithPartitionKey does not exist
// SessionCfg.AllowSchemaMismatch, Whether to allow the schema of uploaded data to be inconsistent with the table schema. The default value is true. When set to false, the Append operation will check the type of uploaded data, and the server will throw a specific exception during Flush.
// SessionCfg.Columns, TODO 作用待明确
func CreateStreamUploadSession(
	projectName, tableName string,
	restClient restclient.RestClient,
	opts ...Option,
) (*StreamUploadSession, error) {
	cfg := newSessionConfig(opts...)

	session := StreamUploadSession{
		ProjectName:         projectName,
		SchemaName:          cfg.SchemaName,
		TableName:           tableName,
		partitionKey:        cfg.PartitionKey,
		Compressor:          cfg.Compressor,
		RestClient:          restClient,
		Columns:             cfg.Columns,
		CreatePartition:     cfg.CreatePartition,
		SlotNum:             cfg.SlotNum,
		schemaVersion:       cfg.SchemaVersion,
		allowSchemaMismatch: cfg.AllowSchemaMismatch,
	}

	req, err := session.newInitiationRequest()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = session.loadInformation(req, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &session, nil
}

func (su *StreamUploadSession) OpenRecordPackWriter() *RecordPackStreamWriter {
	w := newRecordStreamHttpWriter(su)
	return &w
}

func (su *StreamUploadSession) Schema() *tableschema.TableSchema {
	return &su.schema
}

func (su *StreamUploadSession) SchemaVersion() int {
	return su.schemaVersion
}

func (su *StreamUploadSession) newInitiationRequest() (*http.Request, error) {
	queryArgs := make(url.Values, 7)

	if su.partitionKey != "" {
		queryArgs.Set("partition", su.partitionKey)
	}

	if su.CreatePartition {
		queryArgs.Set("create_partition", "")
	}

	if len(su.Columns) > 0 {
		queryArgs.Set("zorder_columns", strings.Join(su.Columns, ","))
	}

	if su.schemaVersion >= 0 {
		queryArgs.Set("schema_version", strconv.Itoa(su.schemaVersion))
	}

	if su.QuotaName != "" {
		queryArgs.Set("quotaName", su.QuotaName)
	}

	headers := getCommonHeaders()
	if su.SlotNum > 0 {
		headers["odps-tunnel-slot-num"] = strconv.Itoa(su.SlotNum)
	}

	resource := su.ResourceUrl()
	req, err := su.RestClient.NewRequestWithParamsAndHeaders(common.HttpMethod.PostMethod, resource, nil, queryArgs, headers)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return req, nil
}

func (su *StreamUploadSession) newReLoadRequest() (*http.Request, error) {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("uploadid", su.id)
	if su.schemaVersion >= 0 {
		queryArgs.Set("schema_version", strconv.Itoa(su.schemaVersion))
	}

	if su.partitionKey != "" {
		queryArgs.Set("partition", su.partitionKey)
	}
	if su.QuotaName != "" {
		queryArgs.Set("quotaName", su.QuotaName)
	}

	headers := getCommonHeaders()

	resource := su.ResourceUrl()
	req, err := su.RestClient.NewRequestWithParamsAndHeaders(common.HttpMethod.PostMethod, resource, nil, queryArgs, headers)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return req, nil
}

func (su *StreamUploadSession) loadInformation(req *http.Request, inited bool) error {
	type ResModel struct {
		CompressMode  string          `json:"compress_mode"`
		FileFormat    string          `json:"file_format"`
		Schema        schemaResModel  `json:"schema"`
		SessionName   string          `json:"session_name"`
		Slots         [][]interface{} `json:"slots"`
		Status        string          `json:"status"`
		QuotaName     string          `json:"quota_name"`
		SchemaVersion int             `json:"schema_version"`
	}

	var resModel ResModel
	var requestId string
	err := su.RestClient.DoWithParseFunc(req, func(res *http.Response) error {
		requestId = res.Header.Get("x-odps-request-id")

		if res.StatusCode/100 != 2 {
			return errors.WithStack(restclient.NewHttpNotOk(res))
		}

		decoder := json.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&resModel))
	})
	if err != nil {
		return errors.WithStack(err)
	}

	if resModel.Status == "init" {
		return errors.Errorf("Session is initiating. RequestId:%s Session ID:%s", requestId, resModel.SessionName)
	}

	if inited {
		tableSchema, err := resModel.Schema.toTableSchema(su.TableName)
		if err != nil {
			return errors.WithStack(err)
		}

		su.id = resModel.SessionName
		su.schema = tableSchema
		su.schemaVersion = resModel.SchemaVersion

		if resModel.QuotaName != "" {
			su.QuotaName = resModel.QuotaName
		}
	}

	slots := make([]slot, len(resModel.Slots))
	for i, rawSlot := range resModel.Slots {
		slots[i], err = newSlot(strconv.Itoa(int(rawSlot[0].(float64))), rawSlot[1].(string))
		if err != nil {
			return errors.WithStack(err)
		}
	}

	su.slotSelector = newSlotSelect(slots)

	return nil
}

func (su *StreamUploadSession) flushStream(streamWriter *RecordPackStreamWriter, timeout time.Duration) (string, int, error) {
	var reader io.ReadCloser
	var writer io.WriteCloser
	reader, writer = io.Pipe()
	currentSlot := su.slotSelector.NextSlot()

	conn, err := su.newUploadConnection(reader, writer, currentSlot, streamWriter.DataSize(), streamWriter.RecordCount(), timeout)
	if err != nil {
		return "", 0, errors.WithStack(err)
	}

	// write bytes to http uploading connection
	_, err = conn.Writer.Write(streamWriter.buffer.Bytes())
	if err != nil {
		// 在write失败时，如果http请求还未完全发送到server, server会等待http请求完成，造成 conn.closeRes()卡住。
		// 因为conn.closeRes()会读取http响应流，而server一直在等剩余的http请求内容。
		// 注意: 这里关掉reader后可能导致writer写数据失败、程序退出而丢失了writer的真实错误原因
		_ = reader.Close()
		// 显示关闭打开的连接，并在http返回非200状态时，获取实际的http错误
		closeError := conn.closeRes()
		if closeError != nil {
			return "", 0, errors.WithStack(closeError)
		}

		return "", 0, errors.WithStack(err)
	}
	// close http writer
	err = conn.Writer.Close()
	if err != nil {
		closeError := conn.closeRes()
		if closeError != nil {
			return "", 0, errors.WithStack(closeError)
		}

		return "", 0, errors.WithStack(err)
	}

	// get and close response
	rOrE := <-conn.resChan

	if rOrE.err != nil {
		return "", 0, errors.WithStack(rOrE.err)
	}

	res := rOrE.res

	if res.StatusCode/100 != 2 {
		return "", 0, errors.WithStack(restclient.NewHttpNotOk(res))
	}

	err = res.Body.Close()
	if err != nil {
		return "", 0, errors.WithStack(err)
	}

	slotNumStr := res.Header.Get(common.HttpHeaderOdpsSlotNum)
	newSlotServer := res.Header.Get(common.HttpHeaderRoutedServer)
	newSlotNum, err := strconv.Atoi(slotNumStr)
	if err != nil {
		return "", 0, errors.WithMessage(err, "invalid slot num get from http odps-tunnel-slot-num header")
	}

	if newSlotNum != su.slotSelector.SlotNum() {
		err = su.reloadSlotNum()
		if err != nil {
			return "", 0, errors.WithStack(err)
		}
	} else if newSlotServer != currentSlot.Server() {
		err := currentSlot.SetServer(newSlotServer)
		if err != nil {
			return "", 0, errors.WithStack(err)
		}
	}
	return res.Header.Get(common.HttpHeaderOdpsRequestId), conn.bytesCount(), nil
}

func (su *StreamUploadSession) newUploadConnection(reader io.ReadCloser, writer io.WriteCloser, currentSlot *slot, dataSize int64, recordCount int64, timeout time.Duration) (*httpConnection, error) {
	queryArgs := make(url.Values, 5)
	queryArgs.Set("uploadid", su.id)
	queryArgs.Set("slotid", currentSlot.id)
	if su.schemaVersion >= 0 {
		queryArgs.Set("schema_version", strconv.Itoa(su.schemaVersion))
	}

	if su.partitionKey != "" {
		queryArgs.Set("partition", su.partitionKey)
	}

	if recordCount > 0 {
		queryArgs.Set("record_count", strconv.FormatInt(recordCount, 10))
	}

	if len(su.Columns) > 0 {
		queryArgs.Set("zorder_columns", strings.Join(su.Columns, ","))
	}
	if su.QuotaName != "" {
		queryArgs.Set("quotaName", su.QuotaName)
	}

	queryArgs.Set("check_latest_schema", strconv.FormatBool(!su.allowSchemaMismatch))

	headers := getCommonHeaders()
	if dataSize < 0 {
		headers[common.HttpHeaderTransferEncoding] = "chunked"
	} else {
		headers[common.HttpHeaderContentLength] = strconv.FormatInt(dataSize, 10)
	}
	headers[common.HttpHeaderContentType] = "application/octet-stream"
	headers[common.HttpHeaderOdpsSlotNum] = strconv.Itoa(su.slotSelector.SlotNum())

	if su.Compressor != nil {
		headers[common.HttpHeaderContentEncoding] = su.Compressor.Name()
	}
	headers[common.HttpHeaderRoutedServer] = currentSlot.Server()

	resource := su.ResourceUrl()
	req, err := su.RestClient.NewRequestWithParamsAndHeaders(common.HttpMethod.PutMethod, resource, reader, queryArgs, headers)
	if err != nil {
		// Close the pipe resources if request creation fails to prevent resource leak
		_ = reader.Close()
		_ = writer.Close()
		return nil, errors.WithStack(err)
	}

	resChan := make(chan resOrErr)
	go func() {
		endpoint := su.RestClient.Endpoint()
		if su.P2PMode {
			defaultEndpoint, _ := url.Parse(su.RestClient.Endpoint())

			newUrl := url.URL{
				Scheme: defaultEndpoint.Scheme,
				Host:   currentSlot.ip,
			}

			endpoint = newUrl.String()
		}

		client := restclient.NewOdpsRestClient(su.RestClient, endpoint)
		client.TcpConnectionTimeout = su.RestClient.TcpConnectionTimeout
		client.HttpTimeout = su.RestClient.HttpTimeout
		if timeout > 0 {
			client.HttpTimeout = timeout
		}

		res, err := client.Do(req)
		resChan <- resOrErr{err: err, res: res}
	}()

	httpConn := newHttpConnection(writer, resChan, su.Compressor)
	return httpConn, nil
}

func (su *StreamUploadSession) reloadSlotNum() error {
	req, err := su.newReLoadRequest()
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(su.loadInformation(req, false))
}

func getCommonHeaders() map[string]string {
	header := make(map[string]string)
	header[common.HttpHeaderOdpsDateTransFrom] = DateTransformVersion
	header[common.HttpHeaderOdpsTunnelVersion] = Version
	header[common.HttpHeaderOdpsSdkSupportSchemaEvolution] = "true"
	return header
}
