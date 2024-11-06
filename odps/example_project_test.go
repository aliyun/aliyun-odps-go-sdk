// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odps_test

import (
	"fmt"
	"log"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func ExampleProjects_List() {
	projectsIns := odpsIns.Projects()
	projects, err := projectsIns.List(odps.ProjectFilter.NamePrefix("p"))
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
	projectName := defaultProjectName

	existed, err := projects.Exists(projectName)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("%s existed: %t", projectName, existed))
	// Output:
}

func ExampleProject() {
	projects := odpsIns.Projects()
	project := projects.Get(defaultProjectName)

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
