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

// List 获取全部的Project
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

	if !project.HasBeLoaded() {
		err := project.Load()

		if err != nil {
			return false, errors.WithStack(err)
		}
	}

	return project.Existed(), nil
}

// CreateExternalProject 创建 external 项目
func (p *Projects) CreateExternalProject(projectName string) error {
	return errors.New("unimplemented")
}

// DeleteExternalProject 删除 external 项目
func (p *Projects) DeleteExternalProject(projectName string) error {
	return errors.New("unimplemented")
}

func (p *Projects) UpdateProject(projectName string) error {
	return errors.New("unimplemented")
}

var ProjectFilter = struct {
	WithNamePrefix func(string) PFilterFunc
	WithOwner      func(string) PFilterFunc
	WithUser       func(string) PFilterFunc
	WithGroup      func(string) PFilterFunc
}{
	// 指定projects的名字前缀作为查询条件
	WithNamePrefix: withProjectNamePrefix,
	// 指定projects的所有者作为查询条件，不能与user同时使用，不支持分页
	WithOwner: withProjectOwner,
	// 指定projects的加入用户名作为查询条件，不能与owner同时使用，不支持分页。
	WithUser: withUserInProject,
	// 指定projects的group name作为查询条件。
	WithGroup: withProjectGroup,
	// MaxItems貌似不起作用
	// MaxItems int
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
