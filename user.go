package odps

import "encoding/xml"

type User struct {
	odpsIns     *Odps
	projectName string
	model       userModel
}

type userModel struct {
	XMLName     xml.Name `xml:"User"`
	ID          string
	DisplayName string
	Comment     string
}

func NewUser(userId string, odpsIns *Odps, projectName ...string) User {
	var _projectName string

	if len(projectName) > 0 {
		_projectName = projectName[0]
	} else {
		_projectName = odpsIns.DefaultProjectName()
	}

	return User{
		odpsIns:     odpsIns,
		projectName: _projectName,
		model:       userModel{ID: userId},
	}
}

func (user *User) Load() error {
	rb := NewResourceBuilder(user.projectName)
	resource := rb.User(user.model.ID)
	client := user.odpsIns.restClient

	return client.GetWithModel(resource, nil, &user.model)
}

func (user *User) ID() string {
	return user.model.ID
}

func (user *User) DisplayName() string {
	return user.model.DisplayName
}

func (user *User) Comment() string {
	return user.model.Comment
}
