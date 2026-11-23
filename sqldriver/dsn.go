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
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/configvalue"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config = odps.Config

var (
	NewConfig        = odps.NewConfig
	NewConfigFromIni = odps.NewConfigFromIni
)

// ParseDSN parses an HTTP(S) endpoint with a required project query parameter.
// Supply a complete AK/SK pair (and optional stsToken) in the DSN, or omit
// credentials and use the ALIBABA_CLOUD_* environment variables as a group.
// tcpConnectionTimeout and httpTimeout are non-negative seconds.
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Fragment != "" {
		return nil, errors.New("invalid URL for ODPS DSN")
	}
	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, errors.New("invalid query parameters in ODPS DSN")
	}
	for key, values := range params {
		if canonical := configvalue.CanonicalTunnelDSNKey(key); key != canonical {
			params[canonical] = append(params[canonical], values...)
			params.Del(key)
		}
	}
	reserved := []string{"project", "stsToken", "tcpConnectionTimeout", "httpTimeout", "tunnelEndpoint", "tunnelQuotaName", "enableLogview"}
	for _, key := range reserved {
		if len(params[key]) > 1 {
			return nil, fmt.Errorf("duplicate %s in ODPS DSN", key)
		}
	}

	config := NewConfig()
	config.ProjectName = params.Get("project")
	if strings.TrimSpace(config.ProjectName) == "" {
		return nil, errors.New("project name is not set")
	}
	user := u.User
	u.User, u.RawQuery, u.ForceQuery = nil, "", false
	if _, err := configvalue.Endpoint(u.String(), "endpoint"); err != nil {
		return nil, err
	}
	config.Endpoint = u.String()
	if user != nil {
		config.AccessId = user.Username()
		config.AccessKey, _ = user.Password()
		config.StsToken = params.Get("stsToken")
	} else {
		if params.Get("stsToken") != "" {
			return nil, errors.New("stsToken in the DSN requires an explicit AccessId and AccessKey")
		}
		config.AccessId = os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
		config.AccessKey = os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
		config.StsToken = os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN")
	}
	if strings.TrimSpace(config.StsToken) == "" {
		config.StsToken = ""
	}
	if strings.TrimSpace(config.AccessId) == "" {
		return nil, errors.New("AccessId is not set")
	}
	if strings.TrimSpace(config.AccessKey) == "" {
		return nil, errors.New("AccessKey is not set")
	}
	config.TunnelEndpoint = params.Get("tunnelEndpoint")
	config.TunnelQuotaName = params.Get("tunnelQuotaName")
	if err := configvalue.SQLTunnelRoute(config.TunnelEndpoint, config.TunnelQuotaName); err != nil {
		return nil, err
	}
	config.TcpConnectionTimeout, err = configvalue.TimeoutSeconds(params.Get("tcpConnectionTimeout"), "tcpConnectionTimeout", config.TcpConnectionTimeout)
	if err != nil {
		return nil, err
	}
	config.HttpTimeout, err = configvalue.TimeoutSeconds(params.Get("httpTimeout"), "httpTimeout", config.HttpTimeout)
	if err != nil {
		return nil, err
	}

	config.Others = make(map[string]string)
	if v := params.Get("enableLogview"); v != "" {
		config.Others["enableLogview"] = v
	}
	for _, key := range reserved {
		params.Del(key)
	}
	config.Hints = make(map[string]string, len(params))
	for key, values := range params {
		config.Hints[key] = values[0]
	}
	return config, nil
}
