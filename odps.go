package odps

type Odps struct {
	defaultProject string

	restClient RestClient
	rb         ResourceBuilder
	projects Projects
}

func NewOdps(account Account) *Odps  {
	ins := Odps {
		restClient: NewOdpsHttpClient(account),
	}

	ins.projects = NewProjects(&ins)

	return &ins
}

func (odps *Odps) DefaultProject() Project  {
	return NewProject(odps.defaultProject, odps)
}

func (odps *Odps) DefaultProjectName() string  {
	return odps.defaultProject
}

func (odps *Odps) SetDefaultProjectName(projectName string)  {
	odps.defaultProject = projectName
	odps.rb.SetProject(projectName)
	odps.restClient.setDefaultProject(projectName)
}

func (odps *Odps) Projects() Projects  {
	return odps.projects
}
