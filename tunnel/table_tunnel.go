package tunnel

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"time"
)

// TableTunnel Tunnel是ODPS的数据通道，用户可以通过Tunnel向ODPS上传或下载数据
// TableTunnel是访问ODPS Tunnel服务的入口类，仅支持表数据(非视图)的上传或下载。
// 对一张表或partition上传下载的过程，成为一个session。session由一个或多个到Tunnel Server的
// HTTP Request组成。session的超时时间是24小时，如果大批量数据传输超过24小时，需要自行拆分成
// 多个session。
// 数据的上传和下载分别由UploadSession和DownloadSession这两个会话来负责
//
// 典型的表数据上传流程如下:
// 1. 创建TableTunnel
// 2. 创建UploadSession
// 3. 创建RecordWriter, 写入Record
// 4. 提交上传操作
//
// 典型的表数据下载流程如下:
// 1. 创建TableTunnel
// 2. 创建DownloadSession
// 3. 创建RecordReader, 读取Record

const DefaultTcpConnectionTimeout = 10 * time.Second

type TableTunnel struct {
	odpsIns              *odps.Odps
	endpoint             string
	httpTimeout          time.Duration
	tcpConnectionTimeout time.Duration
}

func NewTableTunnel(odpsIns *odps.Odps, endpoint string) TableTunnel {
	return TableTunnel{
		odpsIns: odpsIns,
		endpoint: endpoint,
	}
}

func (t *TableTunnel) HttpTimeout() time.Duration {
	return t.httpTimeout
}

func (t *TableTunnel) SetHttpTimeout(httpTimeout time.Duration) {
	t.httpTimeout = httpTimeout
}

func (t *TableTunnel) TcpConnectionTimeout() time.Duration {
	if t.tcpConnectionTimeout == 0 {
		return DefaultTcpConnectionTimeout
	}

	return t.tcpConnectionTimeout
}

func (t *TableTunnel) SetTcpConnectionTimeout(tcpConnectionTimeout time.Duration) {
	t.tcpConnectionTimeout = tcpConnectionTimeout
}

func (t *TableTunnel) CreateUploadSession(projectName, tableName string, opts ...Option) (*UploadSession, error) {
	return CreateUploadSession(projectName, tableName, t.getRestClient(), opts...)
}

func (t *TableTunnel) AttachToExistedUploadSession(
	projectName, tableName, sessionId string,
	opts ...Option) (*UploadSession, error) {
	return AttachToExistedUploadSession(sessionId, projectName, tableName, t.getRestClient(), opts...)
}

func (t *TableTunnel) CreateDownloadSession(projectName, tableName string, opts ...Option) (*DownloadSession, error) {
	return CreateDownloadSession(projectName, tableName, t.getRestClient(), opts...)
}

func (t *TableTunnel) AttachToExistedDownloadSession(
	projectName, tableName, sessionId string,
	opts ...Option) (*DownloadSession, error) {
	return AttachToExistedDownloadSession(sessionId, projectName, tableName, t.getRestClient(), opts...)
}

func (t *TableTunnel) getRestClient() odps.RestClient {
	client := odps.NewOdpsHttpClient(t.odpsIns.Account(), t.endpoint)
	client.HttpTimeout = t.HttpTimeout()
	client.TcpConnectionTimeout = t.TcpConnectionTimeout()

	return client
}
