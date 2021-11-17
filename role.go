package odps

import "encoding/xml"

type Role struct {
	odpsIns     *Odps
	projectName string
	model       roleModel
}

type roleModel struct {
	XMLName xml.Name `xml:"Role"`
	Name    string
	Comment string
}

func NewRole(name string, odpsIns *Odps, projectName ...string) Role {
	var _projectName string

	if len(projectName) > 0 {
		_projectName = projectName[0]
	} else {
		_projectName = odpsIns.DefaultProjectName()
	}

	return Role{
		odpsIns:     odpsIns,
		projectName: _projectName,
		model:       roleModel{Name: name},
	}
}

func (role *Role) Load() error {
	rb := NewResourceBuilder(role.projectName)
	resource := rb.Role(role.model.Name)
	client := role.odpsIns.restClient

	return client.GetWithModel(resource, nil, &role.model)
}

func (role *Role) Name() string {
	return role.model.Name
}

func (role *Role) Comment() string {
	return role.model.Comment
}
