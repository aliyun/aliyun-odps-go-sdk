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
