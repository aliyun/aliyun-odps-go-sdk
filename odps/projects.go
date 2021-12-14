package odps

import (
	"encoding/xml"
	"github.com/pkg/errors"
	"net/url"
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
func (p *Projects) List(filters ...PFilterFunc) ([]Project, error) {
	queryArgs := make(url.Values)

	for _, filter := range filters {
		filter(queryArgs)
	}

	client := p.odpsIns.restClient
	rb := p.odpsIns.rb
	resource := rb.Projects()

	type ResModel struct {
		XMLName  xml.Name `xml:"Projects"`
		Marker   string
		MaxItems int
		Projects []projectModel `xml:"Project"`
	}

	var resModel ResModel
	var projects []Project

	for {
		err := client.GetWithModel(resource, queryArgs, &resModel)
		if err != nil {
			return projects, errors.WithStack(err)
		}

		if len(resModel.Projects) == 0 {
			break
		}

		for _, projectModel := range resModel.Projects {
			project := NewProject(projectModel.Name, p.odpsIns)
			project.model = projectModel

			projects = append(projects, project)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return projects, nil
}

func (p *Projects) GetDefaultProject() Project {
	return p.Get(p.odpsIns.defaultProject)
}

func (p *Projects) Get(projectName string) Project {
	return NewProject(projectName, p.odpsIns)
}

func (p *Projects) Exists(projectName string) (bool, error) {
	project := p.Get(projectName)

	if !project.IsLoaded() {
		err := project.Load()

		if err != nil {
			return false, errors.WithStack(err)
		}
	}

	return project.Existed(), nil
}

// CreateExternalProject  unimplemented!
func (p *Projects) CreateExternalProject(projectName string) error {
	return errors.New("unimplemented")
}

// DeleteExternalProject unimplemented!
func (p *Projects) DeleteExternalProject(projectName string) error {
	return errors.New("unimplemented")
}

func (p *Projects) UpdateProject(projectName string) error {
	return errors.New("unimplemented")
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
