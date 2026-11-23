# MaxCompute SDK for Go

[简体中文](README.zh-CN.md)

Run SQL jobs, inspect catalog metadata, and transfer data in Alibaba Cloud
MaxCompute from Go. The SDK supports `database/sql`, Arrow IPC through Storage
API V2, and row-oriented transfers through Protobuf Tunnel.

[Go API reference](https://pkg.go.dev/github.com/aliyun/aliyun-odps-go-sdk/odps) · [Examples](examples/) · [Release notes](CHANGELOG.md)

## Install

The 1.x SDK requires **Go 1.26.11 or later**. In a new project, first run
`go mod init example.com/maxcompute-quickstart`. The quickstart uses the SQL driver.

```sh
go get github.com/aliyun/aliyun-odps-go-sdk/sqldriver@v1.0.0
```

To try an unreleased revision, replace `v1.0.0` with the commit SHA of that
revision. For applications that require Go 1.15, use [v0.4.27](https://github.com/aliyun/aliyun-odps-go-sdk/releases/tag/v0.4.27).

## Quickstart

Use a MaxCompute project where your account can run SQL and read results. Set
the project name and its service endpoint. The example endpoint is for the
China (Hangzhou) region. [MaxCompute endpoints](https://www.alibabacloud.com/help/en/maxcompute/endpoints)

```sh
export odps_endpoint=https://service.cn-hangzhou.maxcompute.aliyun.com/api
export MAXCOMPUTE_PROJECT=your-project
```

Supply credentials through these environment variables. For other credential
sources, see the [authentication examples](examples/sdk/aliyun_credential/).

| Variable | Value |
| --- | --- |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` | AccessKey ID |
| `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | AccessKey Secret |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | Security token for temporary STS credentials |

Save the following as `main.go` in your Go module.

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
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

	connector, err := sqldriver.NewConnector(client, sqldriver.ConnectorOptions{})
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var answer int64
	err = db.QueryRowContext(ctx, "SELECT CAST(? AS BIGINT) AS answer;", 42).Scan(&answer)
	if err != nil {
		return err
	}
	fmt.Println(answer)
	return nil
}
```

Run `go run .` to submit the query and print `42`. The driver implements
`QueryContext` and `ExecContext`. Transaction and prepare calls return driver
errors. [SQL driver API](https://pkg.go.dev/github.com/aliyun/aliyun-odps-go-sdk/sqldriver)

## Find the right API

| Task | Entry point | Guide |
| --- | --- | --- |
| Run parameterized SQL and scan rows | `sqldriver.NewConnector` and `database/sql` | [SQL driver examples](examples/sql/) |
| Submit, resume, or cancel a SQL job | `odps.Client.SubmitSQL`, `ResumeSQL`, and `SQLJob.Cancel` | [Job lifecycle](docs/v1-migration.md#sql-jobs-and-results) |
| Look up and list catalog metadata | For example, `client.Tables().Get` and `ListPage` | [Client scope and metadata](docs/v1-migration.md#client-and-metadata) |
| Read or write Arrow batches and resume sessions | `odps/storage` | [Storage API V2](docs/storage-v2.md) |
| Upload or download Protobuf records | `odps/tunnel` | [Tunnel examples](examples/sdk/tunnel/) |

## Client configuration

Use `odps.ClientOptions.Schema` to set a default schema. To customize network
behavior, set `ClientOptions.Transport` to a concurrency-safe
`http.RoundTripper`. The SDK signs requests before passing them to the
transport. Its caller manages the connection pool.

## Upgrade from 0.x

The 1.0 migration guide maps the scoped client, catalog, SQL job, and Storage
APIs to their 0.x counterparts. It also covers parameter binding changes
and the move to Arrow Go v18. [Migration guide](docs/v1-migration.md)

## Support and contributing

Report SDK bugs and feature requests in [GitHub Issues](https://github.com/aliyun/aliyun-odps-go-sdk/issues).
For a bug report, include the SDK version, Go version, and a small reproducible
example.

Run `go test ./...` from the repository root. To run the protocol suite against
the pinned MaxCompute Emulator, use `bash scripts/test-emulator.sh` with
Docker and Python 3.9 or later. Pass `--release` to enforce the release coverage
thresholds. [Test script](scripts/test-emulator.sh)

## License

[Apache License 2.0](LICENSE)
