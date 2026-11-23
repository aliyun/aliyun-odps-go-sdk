package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	client, err := odps.NewClient(account.AccountFromEnv(), odps.ClientOptions{
		Endpoint: os.Getenv("MAXCOMPUTE_ENDPOINT"),
		Project:  os.Getenv("MAXCOMPUTE_PROJECT"),
	})
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()

	ctx := context.Background()
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
