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
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
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
	partitionKey    string
	Compressor      Compressor
	RestClient      restclient.RestClient
	Columns         []string
	P2PMode         bool
	CreatePartition bool
	SlotNum         int
	slotSelector    slotSelector
	schema          tableschema.TableSchema
}

func (su *StreamUploadSession) ResourceUrl() string {
	rb := common.NewResourceBuilder(su.ProjectName)
	return rb.TableStream(su.TableName)
}

// CreateStreamUploadSession create a new stream upload session before uploading data。
// The opts can be one or more of:
// SessionCfg.WithPartitionKey
// SessionCfg.WithSchemaName, it doesn't work now
// SessionCfg.WithDefaultDeflateCompressor, using deflate compressor with default level
// SessionCfg.WithDeflateCompressor, using deflate compressor with specific level
// SessionCfg.WithSnappyFramedCompressor
// SessionCfg.SlotNum, 暂不对外开放
// SessionCfg.CreatePartition, create partition if the partition specified by WithPartitionKey does not exist
// SessionCfg.Columns, TODO 作用待明确
func CreateStreamUploadSession(
	projectName, tableName string,
	restClient restclient.RestClient,
	opts ...Option,
) (*StreamUploadSession, error) {
	cfg := newSessionConfig(opts...)

	session := StreamUploadSession{
		ProjectName:     projectName,
		SchemaName:      cfg.SchemaName,
		TableName:       tableName,
		partitionKey:    cfg.PartitionKey,
		Compressor:      cfg.Compressor,
		RestClient:      restClient,
		Columns:         cfg.Columns,
		CreatePartition: cfg.CreatePartition,
		SlotNum:         cfg.SlotNum,
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

func (su *StreamUploadSession) Schema() tableschema.TableSchema {
	return su.schema
}

func (su *StreamUploadSession) newInitiationRequest() (*http.Request, error) {
	queryArgs := make(url.Values, 4)

	if su.partitionKey != "" {
		queryArgs.Set("partition", su.partitionKey)
	}

	if su.CreatePartition {
		queryArgs.Set("create_partition", "")
	}

	if len(su.Columns) > 0 {
		queryArgs.Set("zorder_columns", strings.Join(su.Columns, ","))
	}

	if su.SlotNum > 0 {
		queryArgs.Set("odps-tunnel-slot-num", strconv.Itoa(su.SlotNum))
	}

	resource := su.ResourceUrl()
	req, err := su.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PostMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (su *StreamUploadSession) newReLoadRequest() (*http.Request, error) {
	queryArgs := make(url.Values, 2)
	queryArgs.Set("uploadid", su.id)
	if su.partitionKey != "" {
		queryArgs.Set("partition", su.partitionKey)
	}

	resource := su.ResourceUrl()
	req, err := su.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PostMethod, resource, nil, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	addCommonSessionHttpHeader(req.Header)
	return req, nil
}

func (su *StreamUploadSession) loadInformation(req *http.Request, inited bool) error {
	type ResModel struct {
		CompressMode string          `json:"compress_mode"`
		FileFormat   string          `json:"file_format"`
		Schema       schemaResModel  `json:"Schema"`
		SessionName  string          `json:"session_name"`
		Slots        [][]interface{} `json:"slots"`
		Status       string          `json:"status"`
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
		return errors.Errorf("Session is initiating. RequestId:%s Session name:%s", requestId, resModel.SessionName)
	}

	if inited {
		tableSchema, err := resModel.Schema.toTableSchema(su.TableName)
		if err != nil {
			return errors.WithStack(err)
		}

		su.id = resModel.SessionName
		su.schema = tableSchema
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

func (su *StreamUploadSession) flushStream(streamWriter *RecordPackStreamWriter, timeout time.Duration) (string, error) {
	conn, err := su.newUploadConnection(streamWriter.DataSize(), streamWriter.RecordCount(), timeout)
	if err != nil {
		return "", errors.WithStack(err)
	}

	// close protoc stream writer
	err = streamWriter.protocWriter.Close()
	if err != nil {
		return "", errors.WithStack(err)
	}

	// write bytes to http uploading connection
	_, err = conn.Writer.Write(streamWriter.buffer.Bytes())
	if err != nil {
		return "", errors.WithStack(err)
	}

	// close http writer
	err = conn.Writer.Close()
	if err != nil {
		defer func() {
			rOrE := <-conn.resChan

			if rOrE.err != nil {
				return
			}

			res := rOrE.res
			_ = res.Body.Close()
		}()

		return "", errors.WithStack(err)
	}

	// get and close response
	rOrE := <-conn.resChan

	if rOrE.err != nil {
		return "", errors.WithStack(rOrE.err)
	}

	res := rOrE.res
	err = res.Body.Close()
	if err != nil {
		return "", errors.WithStack(err)
	}

	if res.StatusCode/100 != 2 {
		return "", errors.WithStack(restclient.NewHttpNotOk(res))
	}

	slotNumStr := res.Header.Get(common.HttpHeaderOdpsSlotNum)
	newSlotNum, err := strconv.Atoi(slotNumStr)
	if err != nil {
		return "", errors.WithMessage(err, "invalid slot num get from http odps-tunnel-slot-num header")
	}

	if newSlotNum != su.slotSelector.SlotNum() {
		err = su.reloadSlotNum()
		if err != nil {
			return "", errors.WithStack(err)
		}
	}

	return res.Header.Get(common.HttpHeaderOdpsRequestId), nil
}

func (su *StreamUploadSession) newUploadConnection(dataSize int64, recordCount int64, timeout time.Duration) (*httpConnection, error) {
	queryArgs := make(url.Values, 5)
	queryArgs.Set("uploadid", su.id)
	queryArgs.Set("slotid", su.slotSelector.NextSlot().id)

	if su.partitionKey != "" {
		queryArgs.Set("partition", su.partitionKey)
	}

	if recordCount > 0 {
		queryArgs.Set("record_count", strconv.FormatInt(recordCount, 10))
	}

	if len(su.Columns) > 0 {
		queryArgs.Set("zorder_columns", strings.Join(su.Columns, ","))
	}

	var reader io.ReadCloser
	var writer io.WriteCloser
	reader, writer = io.Pipe()

	resource := su.ResourceUrl()
	req, err := su.RestClient.NewRequestWithUrlQuery(common.HttpMethod.PutMethod, resource, reader, queryArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if dataSize < 0 {
		req.Header.Set(common.HttpHeaderTransferEncoding, "chunked")
	} else {
		req.Header.Set(common.HttpHeaderContentLength, strconv.FormatInt(dataSize, 10))
	}

	req.Header.Set(common.HttpHeaderContentType, "application/octet-stream")
	req.Header.Set(common.HttpHeaderOdpsTunnelVersion, Version)
	req.Header.Set(common.HttpHeaderOdpsSlotNum, strconv.Itoa(su.slotSelector.SlotNum()))

	slot := su.slotSelector.NextSlot()

	if su.Compressor != nil {
		req.Header.Set("Content-Encoding", su.Compressor.Name())
		writer = su.Compressor.NewWriter(writer)
	}

	req.Header.Set(common.HttpHeaderRoutedServer, slot.Server())

	resChan := make(chan resOrErr)
	go func() {
		endpoint := su.RestClient.Endpoint()
		if su.P2PMode {
			defaultEndpoint, _ := url.Parse(su.RestClient.Endpoint())

			newUrl := url.URL{
				Scheme: defaultEndpoint.Scheme,
				Host:   slot.ip,
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

	return &httpConnection{
		Writer:  writer,
		resChan: resChan,
	}, nil
}

func (su *StreamUploadSession) reloadSlotNum() error {
	req, err := su.newReLoadRequest()
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(su.loadInformation(req, false))
}
