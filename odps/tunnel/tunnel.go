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
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/arrow/go/v9/arrow/ipc"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// Tunnel is used to upload or download data in odps, it can also be used to download the result
// of sql query.
// From the begging of one upload or download to the ending is called a session. As some table is
// very big, more than one http connections are used for the upload or download, all the http connections
// are created by session. The timeout of session is 24 hours
//
// The typical table upload processes are
// 1. create tunnel
// 2. create UploadSession
// 3. create RecordWriter, use the writer to write Record data
// 4. commit the data
//
// The typical table download processes are
// 1. create tunnel
// 2. create DownloadSession
// 3. create RecordReader, use the reader to read out Record
type Tunnel struct {
	odpsIns              *odps.Odps
	endpoint             string
	quotaName            string
	httpTimeout          time.Duration
	tcpConnectionTimeout time.Duration
	odpsUserAgent        string
}

// Once the tunnel endpoint is set, it cannot be modified anymore.
func NewTunnel(odpsIns *odps.Odps, endpoint ...string) *Tunnel {
	tunnel := Tunnel{
		odpsIns: odpsIns,
	}
	if len(endpoint) > 0 {
		tunnel.endpoint = endpoint[0]
	}

	return &tunnel
}

func NewTunnelFromProject(project *odps.Project) (*Tunnel, error) {
	endpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		return &Tunnel{}, errors.WithStack(err)
	}

	tunnel := Tunnel{
		odpsIns:  project.OdpsIns(),
		endpoint: endpoint,
	}

	return &tunnel, nil
}

func (t *Tunnel) HttpTimeout() time.Duration {
	return t.httpTimeout
}

func (t *Tunnel) SetHttpTimeout(httpTimeout time.Duration) {
	t.httpTimeout = httpTimeout
}

func (t *Tunnel) TcpConnectionTimeout() time.Duration {
	if t.tcpConnectionTimeout == 0 {
		return DefaultTcpConnectionTimeout
	}

	return t.tcpConnectionTimeout
}

func (t *Tunnel) SetTcpConnectionTimeout(tcpConnectionTimeout time.Duration) {
	t.tcpConnectionTimeout = tcpConnectionTimeout
}

func (t *Tunnel) OdpsUserAgent() string {
	return t.odpsUserAgent
}

func (t *Tunnel) SetOdpsUserAgent(odpsUserAgent string) {
	t.odpsUserAgent = odpsUserAgent
}

func (t *Tunnel) CreateUploadSession(projectName, tableName string, opts ...Option) (*UploadSession, error) {
	client, err := t.getRestClient(projectName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	session, err := CreateUploadSession(projectName, tableName, t.quotaName, client, opts...)

	return session, errors.WithStack(err)
}

func (t *Tunnel) CreateStreamUploadSession(projectName, tableName string, opts ...Option) (*StreamUploadSession, error) {
	client, err := t.getRestClient(projectName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	session, err := CreateStreamUploadSession(projectName, tableName, client, opts...)

	return session, errors.WithStack(err)
}

func (t *Tunnel) AttachToExistedUploadSession(
	projectName, tableName, sessionId string,
	opts ...Option,
) (*UploadSession, error) {
	client, err := t.getRestClient(projectName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	session, err := AttachToExistedUploadSession(sessionId, projectName, tableName, client, opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) CreateDownloadSession(projectName, tableName string, opts ...Option) (*DownloadSession, error) {
	client, err := t.getRestClient(projectName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	session, err := CreateDownloadSession(projectName, tableName, t.quotaName, client, opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) AttachToExistedDownloadSession(
	projectName, tableName, sessionId string,
	opts ...Option,
) (*DownloadSession, error) {
	client, err := t.getRestClient(projectName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	session, err := AttachToExistedDownloadSession(sessionId, projectName, tableName, client, opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) CreateInstanceResultDownloadSession(
	projectName, instanceId string, opts ...InstanceOption,
) (*InstanceResultDownloadSession, error) {
	client, err := t.getRestClient(projectName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	session, err := CreateInstanceResultDownloadSession(projectName, instanceId, t.quotaName, client, opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) getRestClient(projectName string) (restclient.RestClient, error) {
	if t.endpoint == "" {
		project := t.odpsIns.Project(projectName)
		endpoint, err := project.GetTunnelEndpoint(t.quotaName)
		if err != nil {
			return restclient.RestClient{}, errors.WithStack(err)
		}
		t.endpoint = endpoint
	}

	client := restclient.NewOdpsRestClient(t.odpsIns.Account(), t.endpoint)
	client.HttpTimeout = t.HttpTimeout()
	client.TcpConnectionTimeout = t.TcpConnectionTimeout()
	client.SetUserAgent(t.odpsUserAgent)

	return client, nil
}

func (t *Tunnel) GetEndpoint() string {
	return t.endpoint
}

func (t *Tunnel) SetQuotaName(quotaName string) {
	t.quotaName = quotaName
}

func (t *Tunnel) GetQuotaName() string {
	return t.quotaName
}

// Preview table records from table tunnel, max 10000 rows, limit<0 means no limit
func (t *Tunnel) Preview(table *odps.Table, partitionValue string, limit int64) ([]data.Record, error) {
	if limit < 0 {
		limit = -1
	}
	if !table.IsLoaded() {
		err := table.Load()
		if err != nil {
			return nil, err
		}
	}

	projectName := table.ProjectName()
	schemaName := table.SchemaName()
	tableName := table.Name()

	queryArgs := make(url.Values, 2)
	queryArgs.Set("limit", strconv.FormatInt(limit, 10))
	if partitionValue != "" {
		partitionValue = strings.ReplaceAll(partitionValue, "'", "")
		partitionValue = strings.ReplaceAll(partitionValue, "\"", "")
		partitionValue = strings.ReplaceAll(partitionValue, "/", ",")
		queryArgs.Set("partition", partitionValue)
	}
	resource := common.NewResourceBuilder(projectName)
	var resourceUrl string
	resourceUrl = resource.Table(schemaName, tableName)
	resourceUrl += "/preview"

	client, err := t.getRestClient(projectName)
	if err != nil {
		return nil, err
	}
	req, err := client.NewRequestWithUrlQuery(
		common.HttpMethod.GetMethod,
		resourceUrl,
		nil,
		queryArgs,
	)
	if err != nil {
		return nil, err
	}

	addCommonSessionHttpHeader(req.Header)

	req.Header.Set(common.HttpHeaderContentLength, "0")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode/100 != 2 {
		return nil, restclient.NewHttpNotOk(res)
	}

	contentEncoding := res.Header.Get(common.HttpHeaderContentEncoding)
	if contentEncoding != "" {
		res.Body = WrapByCompressor(res.Body, contentEncoding)
	}

	reader, err := ipc.NewReader(res.Body)
	if err != nil {
		return nil, err
	}
	columns := table.Schema().Columns
	partitionColumns := table.Schema().PartitionColumns
	allColumns := append(columns, partitionColumns...)

	var results []data.Record

	for {
		arrowRecord, err := reader.Read()
		isEOF := errors.Is(err, io.EOF)
		if isEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records, err := tableschema.ToMaxComputeRecords(arrowRecord, allColumns, tableschema.ArrowOptionConfig.WithExtendedMode())
		if err != nil {
			return nil, err
		}
		results = append(results, records...)
		arrowRecord.Retain()
	}
	return results, nil
}
