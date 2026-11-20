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
	"context"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/storage"
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
	explicitEndpoint     bool
	quotaName            string
	httpTimeout          time.Duration
	tcpConnectionTimeout time.Duration
	odpsUserAgent        string
	// DisableKeepAlives is passed to the RestClient this Tunnel builds for each
	// session, so sessions that will not reuse their connections do not leave
	// idle ones behind.
	DisableKeepAlives bool
}

// Once the tunnel endpoint is set, it cannot be modified anymore.
func NewTunnel(odpsIns *odps.Odps, endpoint ...string) *Tunnel {
	tunnel := Tunnel{
		odpsIns: odpsIns,
	}
	if len(endpoint) > 0 && endpoint[0] != "" {
		tunnel.endpoint = endpoint[0]
		tunnel.explicitEndpoint = true
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
	client.DisableKeepAlives = t.DisableKeepAlives

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

// Preview reads a sample of table records using Storage API V2.
func (t *Tunnel) Preview(table *odps.Table, partitionValue string, limit int64) ([]data.Record, error) {
	if !table.IsLoaded() {
		if err := table.Load(); err != nil {
			return nil, err
		}
	}
	ctx := context.Background()
	var endpoint string
	if t.explicitEndpoint {
		endpoint = t.endpoint
	}
	client, err := storage.NewClient(ctx, t.odpsIns, storage.ClientOptions{
		Project:              table.ProjectName(),
		TunnelEndpoint:       endpoint,
		QuotaName:            t.quotaName,
		HTTPTimeout:          t.httpTimeout,
		TCPConnectionTimeout: t.tcpConnectionTimeout,
		UserAgent:            t.odpsUserAgent,
		DisableKeepAlives:    t.DisableKeepAlives,
	})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	reader, err := openPreviewReader(ctx, client, table, partitionValue, limit)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	schema := table.Schema()
	columns := slices.Concat(schema.Columns, schema.PartitionColumns)
	var records []data.Record
	for reader.Next() {
		rows, err := tableschema.ToMaxComputeRecords(reader.RecordBatch(), columns)
		if err != nil {
			return nil, err
		}
		records = append(records, rows...)
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func openPreviewReader(ctx context.Context, client *storage.Client, table *odps.Table, partition string, limit int64) (*storage.ArrowReader, error) {
	target := storage.Table{Schema: table.SchemaName(), Name: table.Name()}
	partitionColumns := table.Schema().PartitionColumns
	if len(partitionColumns) == 0 {
		return client.PreviewTable(ctx, target, partition, limit)
	}

	// Storage preview returns data columns. Use a snapshot to include partition
	// columns in the records returned by the original Tunnel API.
	options := storage.TableReadOptions{PartitionColumns: make([]string, len(partitionColumns))}
	for i, column := range partitionColumns {
		options.PartitionColumns[i] = column.Name
	}
	if partition != "" {
		partition = strings.NewReplacer("'", "", "\"", "", "/", ",").Replace(partition)
		options.Partitions = []string{partition}
	}
	snapshot, err := client.CreateTableReadSession(ctx, target, options)
	if err != nil {
		return nil, err
	}
	const maxPreviewRows = 10000
	if limit < 0 || limit > maxPreviewRows {
		limit = maxPreviewRows
	}
	return snapshot.OpenReader(ctx, &storage.RowRange{Count: min(limit, snapshot.RecordCount())})
}
