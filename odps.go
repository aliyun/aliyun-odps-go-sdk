package odps

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
)

type Odps struct {
	defaultProject Project

	httpClient HttpClient
	rb ResourceBuilder
}

func NewOdps(account Account) Odps  {
	return Odps {
		httpClient: NewOdpsHttpClient(account),
	}
}

func (odps *Odps) DefaultProject() Project  {
	return odps.defaultProject
}

func (odps *Odps) SetDefaultProject(project Project)  {
	odps.defaultProject = project
	odps.rb.SetProject(project.Name)
}

func (odps *Odps) Projects()  {
	resource := odps.rb.Projects()
	req, err := odps.httpClient.NewRequest("GET", resource, nil)

	if err != nil {
		println(err)
	}

	res, _ := odps.httpClient.Do(req)

	if res.StatusCode == 200 {
		body, _ := ioutil.ReadAll(res.Body)
		//println(string(body))
		decoder := xml.NewDecoder(bytes.NewReader(body))
		var projects Projects
		var _ = decoder.Decode(&projects)

		for _, p := range projects.Projects {
			fmt.Printf("%+v\n", p)
		}
	}
}