package auto_partition_table

import (
	"log"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
)

func main() {
	// Specify the ini file path
	configPath := "./config.ini"
	conf, err := odps.NewConfigFromIni(configPath)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewApsaraAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	// Set the Default MaxCompute project used By Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	// Create Auto Partition Table
	ins, err := odpsIns.ExecSQl("create table if not exists auto_pt(a bigint, d timestamp) auto partitioned by (trunc_time(d, 'day') as part_value);",
		map[string]string{"odps.sql.type.system.odps2": "true"})
	if err != nil {
		return
	}
	err = ins.WaitForSuccess()
	if err != nil {
		return
	}

	// Load Table
	table := odpsIns.Table("auto_pt")
	err = table.Load()
	if err != nil {
		log.Fatal(err)
	}
	pc := table.PartitionColumns()
	println(pc[0].GenerateExpression.String()) // trunc_time(d, 'day')

	// Get PartitionSpec From Record
	record := data.NewRecord(2)
	record[0] = data.BigInt(1)
	record[1] = data.Timestamp(time.Date(2023, 12, 25, 9, 0, 0, 0, time.UTC))

	schema := table.Schema()
	partitionSpec, err := schema.GeneratePartitionSpec(&record)
	if err != nil {
		log.Fatal(err)
	}
	println(partitionSpec)
}
