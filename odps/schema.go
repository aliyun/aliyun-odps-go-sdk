package odps

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
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
	if isNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// ChangeComment Modify the comment content of the schema.
func (s *Schema) ChangeComment(newComment string) error {
	return s.executeSQL(s.generateChangeCommentSQL(newComment))
}

func (s *Schema) generateChangeCommentSQL(newComment string) string {
	return fmt.Sprintf("alter schema %s.%s set comment %s;", common.QuoteRef(s.ProjectName()), common.QuoteRef(s.Name()), common.QuoteString(newComment))
}

// ChangeOwner Only the Project Owner or users with the Super_Administrator role can execute commands that modify the schema Owner.
func (s *Schema) ChangeOwner(newOwner string) error {
	return s.executeSQL(s.generateChangeOwnerSQL(newOwner))
}

func (s *Schema) generateChangeOwnerSQL(newOwner string) string {
	return fmt.Sprintf("alter schema %s.%s changeowner to %s;", common.QuoteRef(s.ProjectName()), common.QuoteRef(s.Name()), common.QuoteString(newOwner))
}

func (s *Schema) executeSQL(sql string) error {
	return s.Tables().runDDL("AnonymousSQLTask", sql, map[string]string{"odps.namespace.schema": "true"}, nil)
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
	s.beLoaded = false

	model := schemaModel{Name: s.Name(), ProjectName: s.ProjectName()}
	parseFunc := func(res *http.Response) error {
		if err := xml.NewDecoder(res.Body).Decode(&model); err != nil {
			return errors.WithStack(err)
		}

		if value := res.Header.Get(common.HttpHeaderOdpsCreationTime); value != "" {
			var err error
			model.CreateTime, err = http.ParseTime(value)
			if err != nil {
				return errors.Wrap(err, "schema creation time")
			}
		}
		var err error
		model.ModifiedTime, err = http.ParseTime(res.Header.Get(common.HttpHeaderLastModified))
		return errors.Wrap(err, "schema last modified time")
	}
	if err := client.GetWithParseFunc(resource, nil, nil, parseFunc); err != nil {
		return errors.WithStack(err)
	}
	s.model = model
	s.beLoaded = true
	return nil
}

func (s *Schema) ResourceUrl() string {
	rb := common.ResourceBuilder{ProjectName: s.ProjectName()}
	return rb.Schema(s.Name())
}
