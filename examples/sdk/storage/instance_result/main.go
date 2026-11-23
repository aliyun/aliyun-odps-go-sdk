package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/storage"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	credentials := account.AccountFromEnv()
	endpoint := restclient.LoadEndpointFromEnv()
	project := os.Getenv("MAXCOMPUTE_PROJECT")
	if credentials == nil || endpoint == "" || project == "" {
		return fmt.Errorf("set MaxCompute credentials, odps_endpoint, and MAXCOMPUTE_PROJECT")
	}
	service, err := odps.NewClient(credentials, odps.ClientOptions{Endpoint: endpoint, Project: project})
	if err != nil {
		return err
	}
	defer service.CloseIdleConnections()
	client, err := storage.Open(ctx, service, storage.ClientOptions{TunnelEndpoint: os.Getenv("tunnel_odps_endpoint")})
	if err != nil {
		return err
	}
	defer client.Close()

	job, err := service.SubmitSQL(ctx, odps.SQLRequest{Statement: "select cast(1 as bigint) as id"})
	if err != nil {
		return err
	}
	result, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	session, err := client.OpenSQLResult(ctx, result.Ref(), storage.InstanceReadOptions{})
	if err != nil {
		return err
	}
	reader, err := session.OpenReader(ctx, nil)
	if err != nil {
		return err
	}
	defer reader.Release()
	for reader.Next() {
		fmt.Println(reader.RecordBatch())
	}
	return reader.Err()
}
