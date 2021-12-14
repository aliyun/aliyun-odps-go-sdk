package sqldriver

import (
	"errors"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"net/url"
	"strconv"
	"time"
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config struct {
	AccessId         string
	AccessKey        string
	StsToken         string
	Endpoint         string
	ProjectName      string
	ConnectTimeout   time.Duration
	OperationTimeout time.Duration
}

func NewConfig() *Config {
	return &Config{
		ConnectTimeout:   30 * time.Second,
		OperationTimeout: 0,
	}
}

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
			config.ConnectTimeout = time.Duration(n) * time.Second
		}
	}

	if opTimeout != "" {
		n, err := strconv.ParseInt(opTimeout, 10, 32)
		if err != nil {
			config.OperationTimeout = time.Duration(n) * time.Second
		}
	}

	return config, nil
}

func (c *Config) GenAccount() account2.Account {
	var account account2.Account

	if c.StsToken == "" {
		account = account2.NewAliyunAccount(c.AccessId, c.AccessKey)
	} else {
		account = account2.NewStsAccount(c.AccessId, c.AccessKey, c.StsToken)
	}

	return account
}

func (c *Config) GenRestClient() restclient.RestClient {
	account := c.GenAccount()
	client := restclient.NewOdpsRestClient(account, c.Endpoint)
	client.TcpConnectionTimeout = c.ConnectTimeout
	client.HttpTimeout = c.OperationTimeout

	return client
}

func (c *Config) GenOdps() *odps.Odps {
	account := c.GenAccount()
	odpsIns := odps.NewOdps(account, c.Endpoint)
	odpsIns.SetTcpConnectTimeout(c.ConnectTimeout)
	odpsIns.SetHttpTimeout(c.OperationTimeout)
	odpsIns.SetDefaultProjectName(c.ProjectName)

	return odpsIns
}

func (c *Config) FormatDsn() string {
	u, _ := url.Parse(c.Endpoint)

	dsn := url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   u.Path,
	}
	values := make(url.Values)
	values.Set("project", c.ProjectName)
	dsn.RawQuery = values.Encode()
	dsn.User = url.UserPassword(c.AccessId, c.AccessKey)

	return dsn.String()
}
