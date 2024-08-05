package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"log"
	"os"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	sql := "select * from all_types_demo where p1=20 and p2='hangzhou';"

	ins, err := odpsIns.ExecSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	lv := odpsIns.LogView()
	lvUrl, err := lv.GenerateLogView(ins, 10)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(lvUrl)

	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnelIns.CreateInstanceResultDownloadSession(project.Name(), ins.Id())
	if err != nil {
		log.Fatalf("%+v", err)
	}

	start := 0
	step := 9
	total := session.RecordCount()
	schema := session.Schema()

	for start < total {
		end := start + step
		if end >= total {
			end = total
		}

		// 注意：实际读取的record数量不一定小于等于(end - start), 要实际计算一下读出的record数量
		reader, err := session.OpenRecordReader(start, end, 0, nil)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		count := 0
		reader.Iterator(func(record data.Record, err error) {
			if err != nil {
				log.Fatalf("%+v", err)
			}

			count += 1

			for i, d := range record {
				if d == nil {
					fmt.Printf("%s=null", schema.Columns[i].Name)
				} else {
					fmt.Printf("%s=%s", schema.Columns[i].Name, d.Sql())
				}

				if i < record.Len()-1 {
					fmt.Printf(", ")
				} else {
					fmt.Println()
				}
			}
		})

		start += count
	}
}
