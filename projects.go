package odps

import (
	"encoding/xml"
	"errors"
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

// List 获取全部的Project, filter可以忽略或提供一个，提供多个时，只会使用第一个
func (p *Projects) List(c chan Project, filter ...ProjectFilter) error {
	defer close(c)

	queryArgs := make(url.Values)

	if filter != nil {
		filter[0].fillQueryParams(queryArgs)
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

	for {
		err := client.GetWithModel(resource, queryArgs, &resModel);
		if err != nil {
			return err
		}

		if len(resModel.Projects) == 0 {
			break
		}

		for _, projectModel := range resModel.Projects {
			project := NewProject(projectModel.Name, p.odpsIns)
			project.model = projectModel

			c <- project
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

	return nil
}

func (p *Projects) GetDefaultProject() Project {
	return p.Get(p.odpsIns.defaultProject)
}

func (p *Projects) Get(projectName string) Project {
	return NewProject(projectName, p.odpsIns)
}

func (p *Projects) Exists(projectName string) (bool, error) {
	project := p.Get(projectName)

	if ! project.HasBeLoaded() {
		err := project.Load()

		if err != nil {
			return false, err
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

// ProjectFilter 查询所有所有项目(接口:/projects)的过滤条件
type ProjectFilter struct {
	// 指定projects的名字前缀作为查询条件
	Name string
	// 指定projects的所有者作为查询条件，不能与user同时使用，不支持分页
	Owner string
	// 指定projects的加入用户名作为查询条件，不能与owner同时使用，不支持分页。
	User string
	// 指定projects的group name作为查询条件。
	Group string
	// MaxItems貌似不起作用
	// MaxItems int
}

func (f *ProjectFilter) fillQueryParams(params url.Values) {
	if f.Name != "" {
		params.Set("name", f.Name)
	}

	if f.Owner != "" {
		params.Set("owner", f.Owner)
	}

	if f.User != "" {
		params.Set("user", f.User)
	}

	if f.Group != "" {
		params.Set("group", f.Group)
	}
}

