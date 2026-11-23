# MaxCompute Go SDK

[English](README.md)

使用 `odps.Client` 在阿里云 MaxCompute 上运行 SQL 作业、查询元数据。传输数据时，
可通过 Storage API V2 读写 Arrow IPC，也可通过 Tunnel 按行传输 Protobuf 记录。

[Go 接口参考](https://pkg.go.dev/github.com/aliyun/aliyun-odps-go-sdk/odps) · [示例](examples/) · [版本说明](CHANGELOG.md)

## 安装

1.x 版本要求 **Go 1.26.11 或更高版本**。新项目可先运行
`go mod init example.com/maxcompute-quickstart` 初始化模块，再安装原生客户端。

```sh
go get github.com/aliyun/aliyun-odps-go-sdk/odps@v1.0.0
```

试用尚未发布的提交时，将 `v1.0.0` 替换为对应的完整提交哈希。仍需使用 Go 1.15
的应用可选择 [v0.4.27](https://github.com/aliyun/aliyun-odps-go-sdk/releases/tag/v0.4.27)。

## 快速开始

选用当前账号有权执行 SQL 并读取结果的 MaxCompute 项目，设置项目名和服务连接地址。
下面以华东 1（杭州）地域为例。[MaxCompute 连接地址](https://www.alibabacloud.com/help/zh/maxcompute/endpoints)

```sh
export odps_endpoint=https://service.cn-hangzhou.maxcompute.aliyun.com/api
export MAXCOMPUTE_PROJECT=your-project
```

通过下面的环境变量提供凭据。其他凭据来源可参考[认证示例](examples/sdk/aliyun_credential/)。

| 环境变量 | 含义 |
| --- | --- |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` | 访问密钥 ID |
| `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 访问密钥 Secret |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | 使用临时 STS 凭据时填写的安全令牌 |

将以下程序保存为 Go 模块中的 `main.go`。[示例源码](examples/sdk/quickstart/main.go)

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := odps.NewClient(account.AccountFromEnv(), odps.ClientOptions{
		Endpoint: os.Getenv("odps_endpoint"),
		Project:  os.Getenv("MAXCOMPUTE_PROJECT"),
	})
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()

	job, err := client.SubmitSQL(ctx, odps.SQLRequest{Statement: "SELECT 42 AS answer;"})
	if err != nil {
		return err
	}
	result, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	tasks, err := result.Inline(ctx)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		fmt.Print(task.Content())
	}
	return nil
}
```

运行 `go run .`，程序会等待 SQL 作业完成，然后打印查询的内联结果。

```text
answer
42
```

## 按任务选择接口

| 任务 | 入口 | 参考资料 |
| --- | --- | --- |
| 提交、恢复或取消 SQL 作业 | `odps.Client.SubmitSQL`、`ResumeSQL` 和 `SQLJob.Cancel` | [作业生命周期](docs/v1-migration.md#sql-jobs-and-results) |
| 执行带参数的 SQL 并读取行 | `sqldriver.NewConnector` 和 `database/sql` | [SQL 驱动示例](examples/sql/) |
| 查询或分页列举元数据 | 例如 `client.Tables().Get` 和 `ListPage` | [客户端作用域与元数据](docs/v1-migration.md#client-and-metadata) |
| 读写 Arrow 数据批次并恢复会话 | `odps/storage` | [Storage API V2 指南](docs/storage-v2.md) |
| 上传或下载 Protobuf 记录 | `odps/tunnel` | [Tunnel 示例](examples/sdk/tunnel/) |

## 客户端配置

使用 `odps.ClientOptions.Schema` 可设置默认 schema。需要自定义网络行为时，
将 `ClientOptions.Transport` 设为可并发使用的 `http.RoundTripper`。
SDK 在发送前完成请求签名，由传输对象的调用方管理连接池。

## 从 0.x 升级

迁移指南列出了新旧客户端、目录、SQL 作业和 Storage 接口的对应关系，也说明了
SQL 参数绑定的变化和升级到 Arrow Go v18 的方法。[1.0 迁移指南](docs/v1-migration.md)

## 反馈与贡献

通过 [GitHub Issues](https://github.com/aliyun/aliyun-odps-go-sdk/issues)
提交 SDK 问题或功能建议。报告问题时请提供 SDK 版本、Go 版本和最小复现示例。

在仓库根目录运行 `go test ./...` 即可执行 SDK 测试。安装 Docker 和 Python 3.9
或更高版本后，可运行 `bash scripts/test-emulator.sh`，使用固定版本的 MaxCompute
模拟器执行协议测试。加入 `--release` 可检查发布所需的覆盖率。
[测试脚本](scripts/test-emulator.sh)

## 许可证

[Apache 许可证 2.0](LICENSE)
