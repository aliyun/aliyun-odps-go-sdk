package odps

import (
	"encoding/xml"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
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

// NewSchema get specific schema
func NewSchema(odpsIns *Odps, projectName string, schemaName string) *Schema {
	return &Schema{
		model:   schemaModel{ProjectName: projectName, Name: schemaName},
		odpsIns: odpsIns,
	}
}

// Tables return the tables in the schema
func (s *Schema) Tables() *Tables {
	return NewTables(s.odpsIns, s.ProjectName(), s.Name())
}

// Exists check if the schema exists
func (s *Schema) Exists() (bool, error) {
	err := s.Load()

	var httpErr restclient.HttpError
	if errors.As(err, &httpErr) {
		if httpErr.Status == "404 Not Found" {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// Name return the schema name
func (s *Schema) Name() string {
	return s.model.Name
}

// ProjectName return the project name
func (s *Schema) ProjectName() string {
	return s.model.ProjectName
}

// Comment return the schema comment
func (s *Schema) Comment() string {
	return s.model.Comment
}

// Owner return the schema owner
func (s *Schema) Owner() string {
	return s.model.Owner
}

// Type return the schema type
func (s *Schema) Type() string {
	return s.model.Type
}

// CreateTime return the schema create time
func (s *Schema) CreateTime() time.Time {
	return s.model.CreateTime
}

// ModifiedTime return the schema modified time
func (s *Schema) ModifiedTime() time.Time {
	return s.model.ModifiedTime
}

// IsLoaded check if the schema is loaded
func (s *Schema) IsLoaded() bool {
	return s.beLoaded
}

// Load load the schema information
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
