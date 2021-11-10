package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"time"
)


func ExampleProjects_List() {
	// TODO remove the OUTPUT before publish

	filter :=  odps.ProjectFilter{
		Name: "p",
	}
	c := make(chan odps.Project)

	projects := odpsIns.Projects()

	go func() {
		err := projects.List(c, filter)
		if err != nil {
			println(err.Error())
		}
	}()

	for p := range c {
		println(fmt.Sprintf("%+v", p))
	}

	// Output:
}

func ExampleProjects_Exists() {
	projects := odpsIns.Projects()

	existed, _ := projects.Exists("project_1")
	println(existed)

	existed, _ = projects.Exists("project_2")
	println(existed)
	// Output:
}

func ExampleProject()  {
	// TODO remove the OUTPUT before publish

	projects := odpsIns.Projects()
	project := projects.Get("odps_smoke_test")

	if err := project.Load(); err != nil {
		panic(err)
	}

	println(project.Owner())

	creationTime := project.CreationTime()
	println(creationTime.Format(time.RFC1123))

	lastModifiedTime := project.LastModifiedTime()
	println(lastModifiedTime.Format(time.RFC1123))

	defaultCluster, _ := project.GetDefaultCluster()
	println(defaultCluster)

	println("************all properties")
	allProperties, _ := project.GetAllProperties()
	for _, p := range allProperties {
		println(p.Name, p.Value)
	}
	println("************extended properties")
	extendedProperties, _ := project.GetExtendedProperties()
	for _, p := range extendedProperties {
		println(p.Name, p.Value)
	}

	isExisted := project.Existed()
	println(isExisted)
	// Output:
}

