package tunnel

import (
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"time"
)

// Tunnel Tunnel是ODPS的数据通道，用户可以通过Tunnel向ODPS上传或下载数据
// Tunnel是访问ODPS Tunnel服务的入口类，支持表数据(非视图)的上传、下载, 或下载某个instance的执行结果。
// 对一张表或partition上传下载的过程，称为一个session。session由一个或多个到Tunnel Server的
// HTTP Request组成。session的超时时间是24小时，如果大批量数据传输超过24小时，需要自行拆分成
// 多个session。
// 数据的上传和下载分别由UploadSession和DownloadSession这两个会话来负责
//
// 典型的表数据上传流程如下:
// 1. 创建Tunnel
// 2. 创建UploadSession
// 3. 创建RecordWriter, 写入Record
// 4. 提交上传操作
//
// 典型的表数据下载流程如下:
// 1. 创建Tunnel
// 2. 创建DownloadSession
// 3. 创建RecordReader, 读取Record

type Tunnel struct {
	odpsIns              *odps.Odps
	endpoint             string
	httpTimeout          time.Duration
	tcpConnectionTimeout time.Duration
}

func NewTunnel(odpsIns *odps.Odps, endpoint string) Tunnel {
	return Tunnel{
		odpsIns: odpsIns,
		endpoint: endpoint,
	}
}

func NewTunnelFromProject(project odps.Project) (Tunnel, error)  {
	endpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		return Tunnel{}, nil
	}

	tunnel := Tunnel {
		odpsIns: project.OdpsIns(),
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
	return CreateUploadSession(projectName, tableName, t.getRestClient(), opts...)
}

func (t *Tunnel) AttachToExistedUploadSession(
	projectName, tableName, sessionId string,
	opts ...Option) (*UploadSession, error) {
	return AttachToExistedUploadSession(sessionId, projectName, tableName, t.getRestClient(), opts...)
}

func (t *Tunnel) CreateDownloadSession(projectName, tableName string, opts ...Option) (*DownloadSession, error) {
	return CreateDownloadSession(projectName, tableName, t.getRestClient(), opts...)
}

func (t *Tunnel) CreateInstanceResultDownloadSession(
	projectName, instanceId string, opts ...InstanceOption,
) (*InstanceResultDownloadSession, error) {
	return CreateInstanceResultDownloadSession(projectName, instanceId, t.getRestClient(), opts...)
}

func (t *Tunnel) AttachToExistedDownloadSession(
	projectName, tableName, sessionId string,
	opts ...Option) (*DownloadSession, error) {
	return AttachToExistedDownloadSession(sessionId, projectName, tableName, t.getRestClient(), opts...)
}

func (t *Tunnel) getRestClient() odps.RestClient {
	client := odps.NewOdpsHttpClient(t.odpsIns.Account(), t.endpoint)
	client.HttpTimeout = t.HttpTimeout()
	client.TcpConnectionTimeout = t.TcpConnectionTimeout()

	return client
}
