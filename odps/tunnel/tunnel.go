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
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/pkg/errors"
	"time"
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
	httpTimeout          time.Duration
	tcpConnectionTimeout time.Duration
}

func NewTunnel(odpsIns *odps.Odps, endpoint string) Tunnel {
	return Tunnel{
		odpsIns:  odpsIns,
		endpoint: endpoint,
	}
}

func NewTunnelFromProject(project odps.Project) (Tunnel, error) {
	endpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		return Tunnel{}, errors.WithStack(err)
	}

	tunnel := Tunnel{
		odpsIns:  project.OdpsIns(),
		endpoint: endpoint,
	}

	return tunnel, nil
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

func (t *Tunnel) CreateUploadSession(projectName, tableName string, opts ...Option) (*UploadSession, error) {
	session, err := CreateUploadSession(projectName, tableName, t.getRestClient(), opts...)

	return session, errors.WithStack(err)
}

func (t *Tunnel) CreateStreamUploadSession(projectName, tableName string, opts ...Option) (*StreamUploadSession, error) {
	session, err := CreateStreamUploadSession(projectName, tableName, t.getRestClient(), opts...)

	return session, errors.WithStack(err)
}

func (t *Tunnel) AttachToExistedUploadSession(
	projectName, tableName, sessionId string,
	opts ...Option) (*UploadSession, error) {
	session, err := AttachToExistedUploadSession(sessionId, projectName, tableName, t.getRestClient(), opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) CreateDownloadSession(projectName, tableName string, opts ...Option) (*DownloadSession, error) {
	session, err := CreateDownloadSession(projectName, tableName, t.getRestClient(), opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) AttachToExistedDownloadSession(
	projectName, tableName, sessionId string,
	opts ...Option) (*DownloadSession, error) {
	session, err := AttachToExistedDownloadSession(sessionId, projectName, tableName, t.getRestClient(), opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) CreateInstanceResultDownloadSession(
	projectName, instanceId string, opts ...InstanceOption,
) (*InstanceResultDownloadSession, error) {
	session, err := CreateInstanceResultDownloadSession(projectName, instanceId, t.getRestClient(), opts...)
	return session, errors.WithStack(err)
}

func (t *Tunnel) getRestClient() restclient.RestClient {
	client := restclient.NewOdpsRestClient(t.odpsIns.Account(), t.endpoint)
	client.HttpTimeout = t.HttpTimeout()
	client.TcpConnectionTimeout = t.TcpConnectionTimeout()

	return client
}
