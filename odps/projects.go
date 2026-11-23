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

package odps

import (
	"context"
	"net/url"

	"github.com/pkg/errors"
)

type Projects struct {
	odpsIns *Odps
}

func NewProjects(odps *Odps) Projects {
	return Projects{
		odpsIns: odps,
	}
}

// List get all the projects thant current account can access in the specific endpoint
// filters can be specified with ProjectFilter.NamePrefix, ProjectFilter.Owner,
// ProjectFilter.User, ProjectFilter.Group
func (p *Projects) List(filters ...PFilterFunc) ([]*Project, error) {
	queryArgs := make(url.Values)

	for _, filter := range filters {
		filter(queryArgs)
	}

	client := p.odpsIns.restClient

	var projects []*Project

	for {
		page, err := projectsPage(context.Background(), client, queryArgs)
		if err != nil {
			return projects, errors.WithStack(err)
		}

		for _, projectModel := range page.Items {
			project := NewProject(projectModel.Name, p.odpsIns)
			project.model = projectModel

			projects = append(projects, project)
		}

		if page.NextPageToken != "" {
			queryArgs.Set("marker", page.NextPageToken)
		} else {
			break
		}
	}

	return projects, nil
}

func (p *Projects) GetDefaultProject() *Project {
	return p.Get(p.odpsIns.defaultProject)
}

func (p *Projects) Get(projectName string) *Project {
	return NewProject(projectName, p.odpsIns)
}

func (p *Projects) Exists(projectName string) (bool, error) {
	err := p.Get(projectName).Load()
	if isNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

var ProjectFilter = struct {
	// Filter out projects with a name prefix
	NamePrefix func(string) PFilterFunc
	// Filter out projects with project owner name, this filter cannot be used with `User` together
	Owner func(string) PFilterFunc
	// Filter out projects with a project member name
	User func(string) PFilterFunc
	// Filter out projects with the project group name
	Group func(string) PFilterFunc
	// MaxItems, it seems to be not workable
	// WithMaxItems
}{
	NamePrefix: withProjectNamePrefix,
	Owner:      withProjectOwner,
	User:       withUserInProject,
	Group:      withProjectGroup,
}

type PFilterFunc func(url.Values)

func withProjectNamePrefix(name string) PFilterFunc {
	return func(values url.Values) {
		values.Set("name", name)
	}
}

func withProjectOwner(owner string) PFilterFunc {
	return func(values url.Values) {
		values.Set("owner", owner)
	}
}

func withUserInProject(user string) PFilterFunc {
	return func(values url.Values) {
		values.Set("user", user)
	}
}

func withProjectGroup(group string) PFilterFunc {
	return func(values url.Values) {
		values.Set("group", group)
	}
}
