package odps_test

import odps "github.com/aliyun/aliyun-odps-go-sdk"

func ExampleUser()  {
	user := odps.NewUser("1372788524300720", odpsIns, "project_1")
	err := user.Load()
	if err != nil {
		println(err.Error())
	}

	// Output:

}
