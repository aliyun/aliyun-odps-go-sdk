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
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	_ "github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"

	"github.com/pkg/errors"
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
	opts ...Option) (*UploadSession, error) {
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
	opts ...Option) (*DownloadSession, error) {
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

func (t *Tunnel) Preview(projectName, schemaName, tableName string,
	limit int64, opt ...Option) (*http.Response, error) {
	if limit < 0 {
		limit = -1
	}
	//
	cfg := newSessionConfig(opt...)
	//
	queryArgs := make(url.Values, 2)
	queryArgs.Set("limit", strconv.FormatInt(limit, 10))
	if cfg.PartitionKey != "" {
		queryArgs.Set("partition", cfg.PartitionKey)
	}
	resource := common.NewResourceBuilder(projectName)
	var resourceUrl string
	if schemaName != "" {
		resourceUrl = resource.TableWithSchemaName(tableName, schemaName)
	} else {
		resourceUrl = resource.Table(tableName)
	}
	resourceUrl += "/preview"
	//
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
	//
	addCommonSessionHttpHeader(req.Header)
	//
	if cfg.Compressor != nil {
		req.Header.Set(common.HttpHeaderAcceptEncoding, cfg.Compressor.Name())
	}
	req.Header.Set(common.HttpHeaderContentLength, "0")
	//
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode/100 != 2 {
		return nil, restclient.NewHttpNotOk(res)
	}
	//
	contentEncoding := res.Header.Get(common.HttpHeaderContentEncoding)
	if contentEncoding != "" {
		res.Body = WrapByCompressor(res.Body, contentEncoding)
	}
	return res, nil
}

// read table records from tabletunnel, max 10000 rows
func (t *Tunnel) ReadTable(table *odps.Table, partition string, limit int64) (*ArrowStreamRecordReader, error) {
	if !table.IsLoaded() {
		if err := table.Load(); err != nil {
			return nil, err
		}
	}
	//
	tableSchema := table.Schema()
	//if len(columnNames) == 0 {
	//	columnNames = make([]string, len(tableSchema.Columns))
	//	for i, c := range tableSchema.Columns {
	//		columnNames[i] = c.Name
	//	}
	//	if tableSchema.PartitionColumns != nil {
	//		for _, c := range tableSchema.PartitionColumns {
	//			columnNames = append(columnNames, c.Name)
	//		}
	//	}
	//}
	//
	opt := tableschema.ToArrowSchemaOption{
		WithExtensionTimeStamp: true,
		WithPartitionColumns:   true,
	}
	arrowSchema := tableSchema.ToArrowSchema(opt)
	//arrowFields := make([]arrow.Field, 0, len(columnNames))
	//for _, columnName := range columnNames {
	//	fs, ok := arrowSchema.FieldsByName(columnName)
	//	if !ok {
	//		return nil, errors.Errorf("no column names %s in table %s", columnName, table.Name())
	//	}
	//	arrowFields = append(arrowFields, fs...)
	//}
	//arrowSchema = arrow.NewSchema(arrowFields, nil)
	//
	httpResp, err :=
		t.Preview(table.ProjectName(), table.SchemaName(), table.Name(), limit, SessionCfg.WithPartitionKey(partition))
	if err != nil {
		return nil, err
	}

	return newArrowStreamRecordReader(httpResp, arrowSchema)
}
