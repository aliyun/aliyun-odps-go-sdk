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

package sqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"maps"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/configvalue"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func init() {
	sql.Register("odps", &OdpsDriver{})
}

type OdpsDriver struct{}

func (d OdpsDriver) Open(name string) (driver.Conn, error) {
	connector, err := d.connectorFromDSN(name)
	if err != nil {
		return nil, err
	}
	return &connection{connector: connector, closeConnector: true}, nil
}

// OpenConnector validates a DSN and shares one core client across database/sql
// connections. Use NewConnector to provide an existing client programmatically.
func (d OdpsDriver) OpenConnector(name string) (driver.Connector, error) {
	return d.connectorFromDSN(name)
}

func (d OdpsDriver) connectorFromDSN(name string) (*Connector, error) {
	config, err := ParseDSN(name)
	if err != nil {
		return nil, err
	}
	client, err := odps.NewClient(config.GenAccount(), odps.ClientOptions{
		Endpoint: config.Endpoint, Project: config.ProjectName,
		HTTPTimeout: config.HttpTimeout, TCPConnectionTimeout: &config.TcpConnectionTimeout,
	})
	if err != nil {
		return nil, err
	}
	connector, err := NewConnector(client, ConnectorOptions{
		TunnelEndpoint: config.TunnelEndpoint, TunnelQuotaName: config.TunnelQuotaName,
		Hints: config.Hints, EnableLogView: strings.EqualFold(config.Others["enableLogview"], "true"),
	})
	if err != nil {
		return nil, err
	}
	connector.ownsClient = true
	return connector, nil
}

// ConnectorOptions configures SQL hints and the Protobuf result route.
type ConnectorOptions struct {
	TunnelEndpoint  string
	TunnelQuotaName string
	Hints           map[string]string
	EnableLogView   bool
}

// Connector adapts an ODPS client to sql.OpenDB.
type Connector struct {
	client     *odps.Client
	options    ConnectorOptions
	ownsClient bool
}

// NewConnector uses the client's scope, credentials and HTTP transport.
// The caller retains ownership of the client.
func NewConnector(client *odps.Client, options ConnectorOptions) (*Connector, error) {
	if client == nil {
		return nil, fmt.Errorf("sqldriver: an ODPS client is required")
	}
	if err := configvalue.SQLTunnelRoute(options.TunnelEndpoint, options.TunnelQuotaName); err != nil {
		return nil, err
	}
	options.Hints = maps.Clone(options.Hints)
	return &Connector{client: client, options: options}, nil
}

// Connect creates a database/sql connection backed by the shared client.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &connection{connector: c}, nil
}

// Driver returns the ODPS database/sql driver.
func (c *Connector) Driver() driver.Driver { return OdpsDriver{} }

// Close releases idle connections if the connector owns the client.
func (c *Connector) Close() error {
	if c.ownsClient {
		c.client.CloseIdleConnections()
	}
	return nil
}

var (
	_ driver.DriverContext = OdpsDriver{}
	_ driver.Connector     = (*Connector)(nil)
)
