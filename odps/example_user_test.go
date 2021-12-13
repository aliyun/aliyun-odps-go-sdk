package odps_test

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"log"
)

func ExampleUser() {
	user := odps.NewUser("1372788524300720", odpsIns, "project_1")
	err := user.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:

}
