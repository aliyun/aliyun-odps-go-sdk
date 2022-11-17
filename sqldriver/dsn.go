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
	"errors"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"net/url"
	"strconv"
	"time"
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config = odps.Config

var NewConfig = odps.NewConfig
var NewConfigFromIni = odps.NewConfigFromIni

// ParseDSN dsn格式如下
// http://AccessId:AccessKey@host:port/path?project=""&sts_token=""&connTimeout=30&opTimeout=60
// 其中project参数为必填项
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	accessId := u.User.Username()
	if accessId == "" {
		return nil, errors.New("AccessId is not set")
	}

	accessKey, _ := u.User.Password()
	if accessKey == "" {
		return nil, errors.New("AccessKey is not set")
	}

	projectName := u.Query().Get("project")
	if projectName == "" {
		return nil, errors.New("project name is not set")
	}

	endpoint := (&url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   u.Path,
	}).String()

	config := NewConfig()
	config.AccessId = accessId
	config.AccessKey = accessKey
	config.Endpoint = endpoint
	config.ProjectName = projectName

	var connTimeout, opTimeout string

	optionalParams := []string{"stsToken", "connTimeout", "opTimeout"}
	paramPointer := []*string{&config.StsToken, &connTimeout, &opTimeout}
	for i, p := range optionalParams {
		v := u.Query().Get(p)
		if v != "" {
			*paramPointer[i] = v
		}
	}

	if connTimeout != "" {
		n, err := strconv.ParseInt(connTimeout, 10, 32)
		if err != nil {
			config.TcpConnectionTimeout = time.Duration(n) * time.Second
		}
	}

	if opTimeout != "" {
		n, err := strconv.ParseInt(opTimeout, 10, 32)
		if err != nil {
			config.HttpTimeout = time.Duration(n) * time.Second
		}
	}

	return config, nil
}
