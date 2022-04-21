package odps_test

import (
	"fmt"
	"log"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func ExampleInstances_List() {
	ins := odpsIns.Instances()
	timeFormat := "2006-01-02 15:04:05"
	startTime, _ := time.Parse(timeFormat, "2021-11-15 02:15:30")
	endTime, _ := time.Parse(timeFormat, "2021-11-18 06:22:02")

	var f = func(i *odps.Instance, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		println(
			fmt.Sprintf(
				"%s, %s, %s, %s, %s",
				i.Id(), i.Owner(), i.StartTime().Format(timeFormat), i.EndTime().Format(timeFormat), i.Status(),
			))
	}
	ins.List(f, odps.InstanceFilter.TimeRange(startTime, endTime))

	// Output:
}

func ExampleInstances_ListInstancesQueued() {
	ins := odpsIns.Instances()

	instances, err := ins.ListInstancesQueued()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, i := range instances {
		println(fmt.Sprintf("%+v", i))
	}

	// Output:
}

func ExampleInstances_CreateTask() {
	instances := odpsIns.Instances()
	sqlTask := odps.NewSqlTask("hello", "select count(*) from sale_detail;", "", nil)
	instance, err := instances.CreateTask(defaultProjectName, &sqlTask)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(instance.Id())

	err = instance.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%s, %s, %s", instance.StartTime(), instance.EndTime(), instance.Status()))

	timeFormat := "2006-01-02 15:04:05"

Loop:
	for {
		tasks, err := instance.GetTasks()
		task := tasks[0]
		if err != nil {
			log.Fatalf("%+v", err)
		}

		println(
			fmt.Sprintf(
				"%s, %s, %s, %s",
				task.StartTime.Format(timeFormat), task.EndTime.Format(timeFormat), task.Status, task.Name,
			))

		switch task.Status {
		case odps.TaskCancelled, odps.TaskFailed, odps.TaskSuccess:
			break Loop
		}

		time.Sleep(time.Second * 2)
	}

	err = instance.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	body, err := instance.GetTaskDetail("hello")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(string(body))

	println(
		fmt.Sprintf(
			"%s, %s, %s",
			instance.StartTime().Format(timeFormat), instance.EndTime().Format(timeFormat), instance.Status(),
		))

	// Output:
}

func ExampleInstance_Terminate() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello", "select count(*) from user;", "", nil)
	instance, err := instances.CreateTask(defaultProjectName, &sqlTask)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(instance.Id())

	err = instance.Terminate()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = instance.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%s, %s, %s", instance.StartTime(), instance.EndTime(), instance.Status()))

	// Output:
}

func ExampleInstance_GetTaskProgress() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello", "select count(*) from sale_detail;", "", nil)
	instance, err := instances.CreateTask(defaultProjectName, &sqlTask)

	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(instance.Id())

	for i := 0; i < 5; i++ {
		progress, err := instance.GetTaskProgress("hello")
		if err != nil {
			log.Fatalf("%+v", err)
		}

		for _, stage := range progress {
			println(fmt.Sprintf("%+v", stage))

		}

		time.Sleep(time.Second * 1)
	}

	body, err := instance.GetTaskDetail("hello")
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(string(body))

	// Output:
}

func ExampleInstance_GetTaskSummary() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello1", "select count(*) from sale_detail;", "", nil)
	instance, err := instances.CreateTask(defaultProjectName, &sqlTask)

	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(instance.Id())
	_ = instance.WaitForSuccess()

	taskSummary, err := instance.GetTaskSummary("hello1")
	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(fmt.Sprintf("%s\n%s\n", taskSummary.JsonSummary, taskSummary.Summary))

	// Output:
}

func ExampleInstance_GetCachedInfo() {
	instances := odps.NewInstances(odpsIns)
	sqlTask := odps.NewSqlTask("hello1", "select * from user;", "", nil)
	instance, err := instances.CreateTask(defaultProjectName, &sqlTask)

	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(instance.Id())

	i, err := instance.GetCachedInfo()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%+v", i))

	// Output:
}
