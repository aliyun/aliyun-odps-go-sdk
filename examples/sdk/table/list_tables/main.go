package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
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

	ts := odpsIns.Tables()
	ts.List(
		func(t *odps.Table, err error) {
			if err != nil {
				log.Fatalf("%+v", err)
			}

			println(fmt.Sprintf("%s, %s, %s", t.Name(), t.Owner(), t.Type()))
		},
		odps.TableFilter.Extended(),
		odps.TableFilter.Type(odps.ManagedTable),
	)
}
