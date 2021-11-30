package odps

type Odps struct {
	defaultProject string

	account    Account
	restClient RestClient
	rb         ResourceBuilder
	projects   Projects
}

func NewOdps(account Account, endpoint string) *Odps {
	ins := Odps{
		account: account,
		restClient: NewOdpsHttpClient(account, endpoint),
	}

	ins.projects = NewProjects(&ins)

	return &ins
}

func (odps *Odps) Account() Account {
	return odps.account
}

func (odps *Odps) RestClient() RestClient {
	return odps.restClient
}

func (odps *Odps) DefaultProject() Project {
	return NewProject(odps.defaultProject, odps)
}

func (odps *Odps) DefaultProjectName() string {
	return odps.defaultProject
}

func (odps *Odps) SetDefaultProjectName(projectName string) {
	odps.defaultProject = projectName
	odps.rb.SetProject(projectName)
	odps.restClient.setDefaultProject(projectName)
}

func (odps *Odps) Projects() Projects {
	return odps.projects
}
