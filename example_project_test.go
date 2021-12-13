package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"log"
	"time"
)

func ExampleProjects_List() {
	projectsIns := odpsIns.Projects()
	projects, err := projectsIns.List(odps.ProjectFilter.WithNamePrefix("p"))

	if err != nil {
		log.Fatalf("%+v", err)
	}

	for _, project := range projects {
		println(fmt.Sprintf("%+v", project))
	}

	// Output:
}

func ExampleProjects_Exists() {
	projects := odpsIns.Projects()
	projectName := "project_1"

	existed, err := projects.Exists(projectName)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%s existed: %t", projectName, existed))
	// Output:
}

func ExampleProject() {
	projects := odpsIns.Projects()
	project := projects.Get("odps_smoke_test")

	if err := project.Load(); err != nil {
		panic(err)
	}

	println(project.Status())
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

func ExampleProject_GetTunnelEndpoint() {
	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	} else {
		println(tunnelEndpoint)
	}

	// Output:
}
