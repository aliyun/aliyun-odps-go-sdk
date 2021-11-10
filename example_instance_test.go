package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"time"
)

func ExampleInstances_List() {
	instances := odps.NewInstances(odpsIns)

	c := make(chan odps.Instance)

	go func() {
		err := instances.List(c)
		if err != nil {
			println(err.Error())
		}
	}()

	for i := range c {
		println(fmt.Sprintf("%+v", i))
	}

	// Output:
}

func ExampleInstances_ListInstancesQueued() {
	instances := odps.NewInstances(odpsIns)

	c := make(chan string)

	go func() {
		err := instances.ListInstancesQueued(c)
		if err != nil {
			println(err.Error())
		}
	}()

	for i := range c {
		println(fmt.Sprintf("%+v", i))
	}

	// Output:
}

func ExampleInstances_CreateTask() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello", "select count(*) from user;", "", nil)
	instance, err := instances.CreateTask("odps_smoke_test", &sqlTask)

	if err != nil {
		println(err.Error())
	} else {
		println(instance.Id())
	}

	err = instance.Load()
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%s, %s, %s", instance.StartTime(), instance.EndTime(), instance.Status()))
	}

	body, err := instance.GetTaskDetail("hello")
	if err != nil {
		println(err.Error())
	} else {
		println(body)
	}

Loop:
	for {
		tasks, err := instance.GetTasks()
		task := tasks[0]
		if err != nil {
			println(err.Error())
			break
		} else {
			println(fmt.Sprintf("%s, %s, %s, %s", task.StartTime, task.EndTime, task.Status, task.Name))
		}

		switch task.Status {
		case odps.TaskCancelled, odps.TaskFailed, odps.TaskSuccess:
			break Loop
		}

		time.Sleep(time.Second * 2)
	}

	err = instance.Load()
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%s, %s, %s", instance.StartTime(), instance.EndTime(), instance.Status()))
	}

	// Output:
}

func ExampleInstance_Terminate() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello", "select count(*) from user;", "", nil)
	instance, err := instances.CreateTask("odps_smoke_test", &sqlTask)

	if err != nil {
		println(err.Error())
	} else {
		println(instance.Id())
	}

	err = instance.Terminate()
	if err != nil {
		println(err.Error())
	}

	err = instance.Load()
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%s, %s, %s", instance.StartTime(), instance.EndTime(), instance.Status()))
	}

	// Output:
}

func ExampleInstance_GetTaskProgress() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello", "select name, age from jet_pai_smoke_input, user where jet_pai_smoke_input.label = user.name;", "", nil)
	instance, err := instances.CreateTask("odps_smoke_test", &sqlTask)

	if err != nil {
		println(err.Error())
	} else {
		println(instance.Id())
	}

	body, err := instance.GetTaskDetail("hello")
	if err != nil {
		println(err.Error())
	} else {
		println(body)
	}

	for i := 0; i < 5; i++ {
		progress, err := instance.GetTaskProgress("hello")
		if err != nil {
			println(err.Error())
		}

		for _, stage := range progress {
			println(fmt.Sprintf("%+v", stage))

		}

		time.Sleep(time.Second * 1)
	}

	// Output:
}

func ExampleInstance_GetTaskSummary() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello1", "select * from user;", "", nil)
	instance, err := instances.CreateTask("odps_smoke_test", &sqlTask)

	if err != nil {
		println(err.Error())
	} else {
		println(instance.Id())
	}

	taskSummary, err := instance.GetTaskSummary("hello1")
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%s\n%s\n", taskSummary.JsonSummary, taskSummary.Summary))
	}

	// Output:
}


func ExampleInstance_GetQueueingInfo() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello1", "select * from user;", "", nil)
	instance, err := instances.CreateTask("odps_smoke_test", &sqlTask)

	if err != nil {
		println(err.Error())
	} else {
		println(instance.Id())
	}

	i, err := instance.GetCachedInfo()
	if err != nil {
		println(err.Error())
	} else {
		println(fmt.Sprintf("%+v", i))
	}

	// Output:
}
