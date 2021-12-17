package odps_test

import (
	"fmt"
	"log"
	"time"
)

func ExampleOdps_RunSQl() {
	t := time.Now()
	ts := t.Format("2006-01-02")
	sql := fmt.Sprintf("insert into has_date values (date'%s');", ts)

	ins, err := odpsIns.ExecSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	result, err := ins.GetResult()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, r := range result {
		println(r.Result)
	}

	// Output:
}
