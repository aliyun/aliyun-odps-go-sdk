package odps

import (
	"bytes"
	"encoding/xml"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

// Functions is used for the functions in odps projects
type Functions struct {
	projectName string
	schemaName  string
	OdpsIns     *Odps
}

// NewFunctions create a new Functions
func NewFunctions(OdpsIns *Odps) *Functions {
	return &Functions{
		projectName: OdpsIns.DefaultProjectName(),
		schemaName:  OdpsIns.CurrentSchemaName(),
		OdpsIns:     OdpsIns,
	}
}

// Create create a function
func (f *Functions) Create(projectName, schemaName string, function Function) error {
	if projectName == "" {
		projectName = f.projectName
	}
	if schemaName == "" {
		schemaName = f.schemaName
	}
	if function.Name() == "" {
		return errors.New("function name cannot be empty")
	}

	rb := common.NewResourceBuilder(projectName)
	resource := rb.Functions()

	headers := make(map[string]string)
	headers["Content-Type"] = "application/xml"

	queryArgs := make(url.Values, 4)
	if schemaName != "" {
		queryArgs.Set("curr_schema", schemaName)
	}

	data, err := xml.Marshal(function.Model)
	if err != nil {
		return errors.WithStack(err)
	}
	dataWithReader := []byte(xml.Header + string(data))

	client := f.OdpsIns.restClient
	req, err := client.NewRequestWithParamsAndHeaders(common.HttpMethod.PostMethod, resource, bytes.NewBuffer(dataWithReader), queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}
	client.HttpTimeout = 60 * time.Second
	_, err = client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// Update update a function
func (f *Functions) Update(projectName, schemaName string, function Function) error {
	if projectName == "" {
		projectName = f.projectName
	}
	if schemaName == "" {
		schemaName = f.schemaName
	}
	if function.Name() == "" {
		return errors.New("function name cannot be empty")
	}

	rb := common.NewResourceBuilder(projectName)
	resource := rb.Function(function.Name())

	headers := make(map[string]string)
	headers["Content-Type"] = "application/xml"

	queryArgs := make(url.Values, 4)
	if schemaName != "" {
		queryArgs.Add("schema", schemaName)
	}

	data, err := xml.Marshal(function.Model)
	if err != nil {
		return errors.WithStack(err)
	}
	dataWithReader := []byte(xml.Header + string(data))

	client := f.OdpsIns.restClient
	req, err := client.NewRequestWithParamsAndHeaders(common.HttpMethod.PutMethod, resource, bytes.NewBuffer(dataWithReader), queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}
	client.HttpTimeout = 60 * time.Second
	_, err = client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// Get get a function
func (f *Functions) Get(functionName string) (*Function, error) {
	if functionName == "" {
		return nil, errors.New("function name is not set")
	}

	fb := NewFunctionBuilder()
	function := fb.ProjectName(f.projectName).Odps(f.OdpsIns).Name(functionName).Build()
	return &function, nil
}

// Delete delete a function
func (f *Functions) Delete(functionName string) error {
	if functionName == "" {
		return errors.New("function name is not set")
	}

	rb := common.NewResourceBuilder(f.projectName)
	resource := rb.Function(functionName)

	queryArgs := make(url.Values, 4)
	if f.SchemaName() != "" {
		queryArgs.Set("curr_schema", f.SchemaName())
	}

	client := f.OdpsIns.restClient
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.DeleteMethod, resource, nil, queryArgs)
	if err != nil {
		return errors.WithStack(err)
	}
	client.HttpTimeout = 60 * time.Second
	_, err = client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// SchemaName get the schema name
func (f *Functions) SchemaName() string {
	return f.schemaName
}

// ProjectName get the project name
func (f *Functions) ProjectName() string {
	return f.projectName
}
