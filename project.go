package odps

type Project struct {
	Name               string     `xml:"Name"`
	Type               string     `xml:"Type"`
	Comment            string     `xml:"Comment"`
	State              string     `xml:"State"`
	ProjectGroupName   string     `xml:"ProjectGroupName"`
	Properties         []Property `xml:"Properties"`
	ExtendedProperties []Property `xml:"ExtendedProperties"`
	DefaultCluster     string     `xml:"DefaultCluster"`
	Clusters           string     `xml:"Clusters"`
	Owner              string     `xml:"Owner"`
	CreationTime       string     `xml:"CreationTime"`
	LastModifiedTime   string     `xml:"LastModifiedTime"`

	rb ResourceBuilder
}

type Property struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type Cluster struct {
	QuotaID    string     `xml:"Name"`
	Properties []Property `xml:"Properties"`
}

func (p *Project) baseUrlPath() string {
	if p.rb.projectName == "" {
		p.rb.projectName = p.Name
	}

	return p.rb.Project()
}
