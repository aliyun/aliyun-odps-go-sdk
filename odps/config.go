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

package odps

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/ini.v1"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/configvalue"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// Config is the basic config for odps. The NewConfig function should be used, which sets default values.
type Config struct {
	AccessId             string
	AccessKey            string
	StsToken             string
	Endpoint             string
	ProjectName          string
	TcpConnectionTimeout time.Duration
	HttpTimeout          time.Duration
	TunnelEndpoint       string
	TunnelQuotaName      string
	Hints                map[string]string
	Others               map[string]string
}

func NewConfig() *Config {
	return &Config{
		TcpConnectionTimeout: 30 * time.Second,
		HttpTimeout:          0,
	}
}

func NewConfigFromIni(iniPath string) (*Config, error) {
	cfg, err := ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, iniPath)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	section := cfg.Section("odps")
	conf := NewConfig()

	requiredParams := []string{
		"access_id",
		"access_key",
		"endpoint",
		"project",
	}
	paramPointer := []*string{
		&conf.AccessId,
		&conf.AccessKey,
		&conf.Endpoint,
		&conf.ProjectName,
	}
	for i, p := range requiredParams {
		key, err := section.GetKey(p)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		*paramPointer[i] = key.String()
		section.DeleteKey(p)
	}

	if strings.TrimSpace(conf.ProjectName) == "" || strings.TrimSpace(conf.AccessId) == "" || strings.TrimSpace(conf.AccessKey) == "" {
		return nil, errors.New("project and complete access credentials are required")
	}
	if _, err := configvalue.Endpoint(conf.Endpoint, "endpoint"); err != nil {
		return nil, err
	}

	var connTimeout, httpTimeout string
	optionalParams := []string{
		"sts_token",
		"tcp_connection_timeout",
		"http_timeout",
		"tunnel_endpoint",
		"tunnel_quota_name",
	}
	paramPointer = []*string{
		&conf.StsToken,
		&connTimeout,
		&httpTimeout,
		&conf.TunnelEndpoint,
		&conf.TunnelQuotaName,
	}
	for i, p := range optionalParams {
		if key, err := section.GetKey(p); err == nil {
			*paramPointer[i] = key.String()
			section.DeleteKey(p)
		}
	}

	conf.TcpConnectionTimeout, err = configvalue.TimeoutSeconds(connTimeout, "tcp_connection_timeout", conf.TcpConnectionTimeout)
	if err != nil {
		return nil, err
	}
	conf.HttpTimeout, err = configvalue.TimeoutSeconds(httpTimeout, "http_timeout", conf.HttpTimeout)
	if err != nil {
		return nil, err
	}
	if conf.TunnelEndpoint != "" {
		if _, err := configvalue.Endpoint(conf.TunnelEndpoint, "tunnel_endpoint"); err != nil {
			return nil, err
		}
	}

	otherParams := []string{"enableLogview"}
	conf.Others = make(map[string]string)
	for _, p := range otherParams {
		if key, err := section.GetKey(p); err == nil {
			conf.Others[p] = key.String()
			section.DeleteKey(p)
		}
	}

	conf.Hints = make(map[string]string)
	keys := section.Keys()
	for _, key := range keys {
		hint := strings.TrimPrefix(key.Name(), "hints.")
		conf.Hints[hint] = key.Value()
	}

	return conf, nil
}

func (c *Config) GenAccount() account2.Account {
	var account account2.Account

	if strings.TrimSpace(c.StsToken) == "" {
		account = account2.NewAliyunAccount(c.AccessId, c.AccessKey)
	} else {
		account = account2.NewStsAccount(c.AccessId, c.AccessKey, c.StsToken)
	}

	return account
}

func (c *Config) GenRestClient() restclient.RestClient {
	account := c.GenAccount()
	client := restclient.NewOdpsRestClient(account, c.Endpoint)
	client.TcpConnectionTimeout = c.TcpConnectionTimeout
	client.HttpTimeout = c.HttpTimeout

	return client
}

func (c *Config) GenOdps() *Odps {
	account := c.GenAccount()
	odpsIns := NewOdps(account, c.Endpoint)
	odpsIns.SetTcpConnectTimeout(c.TcpConnectionTimeout)
	odpsIns.SetHttpTimeout(c.HttpTimeout)
	odpsIns.SetDefaultProjectName(c.ProjectName)

	return odpsIns
}

// FormatDsn formats c as a DSN, returning an empty string if c is invalid.
//
// Deprecated: use FormatDSN to receive validation errors.
func (c *Config) FormatDsn() string {
	dsn, _ := c.FormatDSN()
	return dsn
}

// FormatDSN validates and formats c as a DSN. The result contains credentials
// when they are set in c and must be handled as a secret.
func (c *Config) FormatDSN() (string, error) {
	if c == nil {
		return "", errors.New("ODPS config is required")
	}
	dsn, err := configvalue.Endpoint(c.Endpoint, "endpoint")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(c.ProjectName) == "" {
		return "", errors.New("project name is not set")
	}
	explicit := c.AccessId != "" || c.AccessKey != "" || strings.TrimSpace(c.StsToken) != ""
	if explicit && (strings.TrimSpace(c.AccessId) == "" || strings.TrimSpace(c.AccessKey) == "") {
		return "", errors.New("incomplete explicit ODPS credentials")
	}
	if err := configvalue.SQLTunnelRoute(c.TunnelEndpoint, c.TunnelQuotaName); err != nil {
		return "", err
	}
	for name, timeout := range map[string]time.Duration{
		"http timeout": c.HttpTimeout, "TCP connection timeout": c.TcpConnectionTimeout,
	} {
		if timeout < 0 || timeout%time.Second != 0 || timeout/time.Second > 1<<31-1 {
			return "", errors.Errorf("invalid %s: expected a whole number of seconds in [0, 2147483647]", name)
		}
	}
	values := make(url.Values)
	for k, v := range c.Hints {
		values.Set(configvalue.CanonicalTunnelDSNKey(k), v)
	}
	for k, v := range c.Others {
		values.Set(configvalue.CanonicalTunnelDSNKey(k), v)
	}
	// The typed fields always determine the config, even when empty.
	for _, key := range []string{"project", "stsToken", "httpTimeout", "tcpConnectionTimeout", "tunnelQuotaName", "tunnelEndpoint"} {
		values.Del(key)
	}
	values.Set("project", c.ProjectName)
	if strings.TrimSpace(c.StsToken) != "" {
		values.Set("stsToken", c.StsToken)
	}
	if c.HttpTimeout > 0 {
		values.Set("httpTimeout", strconv.FormatInt(int64(c.HttpTimeout/time.Second), 10))
	}
	if c.TcpConnectionTimeout != NewConfig().TcpConnectionTimeout {
		values.Set("tcpConnectionTimeout", strconv.FormatInt(int64(c.TcpConnectionTimeout/time.Second), 10))
	}
	if c.TunnelQuotaName != "" {
		values.Set("tunnelQuotaName", c.TunnelQuotaName)
	}
	if c.TunnelEndpoint != "" {
		values.Set("tunnelEndpoint", c.TunnelEndpoint)
	}
	dsn.RawQuery = values.Encode()
	if c.AccessId != "" {
		dsn.User = url.UserPassword(c.AccessId, c.AccessKey)
	}
	return dsn.String(), nil
}
