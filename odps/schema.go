package odps

import (
	"encoding/xml"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/pkg/errors"
	"net/http"
	"time"
)

// Schema represent the namespace schema in odps projects
type Schema struct {
	model    schemaModel
	odpsIns  *Odps
	beLoaded bool
}

type schemaModel struct {
	XMLName      xml.Name `xml:"Schema"`
	Name         string
	ProjectName  string `xml:"Project"`
	Comment      string
	Type         string
	CreateTime   time.Time `xml:"NoParsed"`
	ModifiedTime time.Time `xml:"NoParsed2"`
	IfNotExists  bool
	Owner        string
}

func NewSchema(odpsIns *Odps, projectName string, schemaName string) *Schema {
	return &Schema{
		model:   schemaModel{ProjectName: projectName, Name: schemaName},
		odpsIns: odpsIns,
	}
}

func (s *Schema) Name() string {
	return s.model.Name
}

func (s *Schema) ProjectName() string {
	return s.model.ProjectName
}

func (s *Schema) Comment() string {
	return s.model.Comment
}

func (s *Schema) Owner() string {
	return s.model.Owner
}

func (s *Schema) Type() string {
	return s.model.Type
}

func (s *Schema) CreateTime() time.Time {
	return s.model.CreateTime
}

func (s *Schema) ModifiedTime() time.Time {
	return s.model.ModifiedTime
}

func (s *Schema) IsLoaded() bool {
	return s.beLoaded
}

func (s *Schema) Load() error {
	client := s.odpsIns.restClient
	resource := s.ResourceUrl()
	s.beLoaded = true

	var GMT, _ = time.LoadLocation("GMT")
	parseFunc := func(res *http.Response) error {
		decoder := xml.NewDecoder(res.Body)
		err := decoder.Decode(&s.model)
		if err != nil {
			return errors.WithStack(err)
		}

		createTime, err := time.ParseInLocation(time.RFC1123, res.Header.Get(common.HttpHeaderOdpsCreationTime), GMT)
		s.model.CreateTime = createTime

		lastModifiedTime, err := time.ParseInLocation(time.RFC1123, res.Header.Get(common.HttpHeaderLastModified), GMT)
		s.model.ModifiedTime = lastModifiedTime

		return errors.WithStack(err)
	}
	err := client.GetWithParseFunc(resource, nil, parseFunc)
	return errors.WithStack(err)
}

func (s *Schema) ResourceUrl() string {
	rb := common.ResourceBuilder{ProjectName: s.ProjectName()}
	return rb.Schema(s.Name())
}
