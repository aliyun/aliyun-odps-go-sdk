package odps

import (
	"encoding/xml"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// Function is a function
type Function struct {
	ProjectName string
	beLoaded    bool
	OdpsIns     *Odps
	Model       functionModel
}

type functionModel struct {
	XMLName            xml.Name    `xml:"Function"`
	SchemaName         string      `xml:"SchemaName,omitempty"`
	Name               string      `xml:"Alias,omitempty"`
	Owner              string      `xml:"Owner,omitempty"`
	CreationTime       string      `xml:"CreationTime,omitempty"`
	ClassPath          string      `xml:"ClassType,omitempty"`
	Resources          []FResource `xml:"Resources,omitempty"`
	IsSQLFunction      bool        `xml:"IsSqlFunction,omitempty"`
	SQLDefinitionText  string      `xml:"SqlDefinitionText,omitempty"`
	IsEmbeddedFunction bool        `xml:"IsEmbeddedFunction,omitempty"`
	ProgramLanguage    string      `xml:"ProgramLanguage,omitempty"`
	Code               string      `xml:"Code,omitempty"`
	FileName           string      `xml:"FileName,omitempty"`
}

// FResource is a resource for function
type FResource struct {
	ResourceName string `xml:"ResourceName,omitempty"`
}

// Load function meta from MaxCompute
func (f *Function) Load() error {
	rb := common.NewResourceBuilder(f.ProjectName)
	resource := rb.Function(f.Name())

	queryArgs := make(url.Values, 4)
	if f.SchemaName() != "" {
		queryArgs.Set("curr_schema", f.SchemaName())
	}

	client := f.OdpsIns.restClient
	err := client.GetWithModel(resource, queryArgs, &f.Model)
	if err != nil {
		return errors.WithStack(err)
	}

	f.beLoaded = true
	return nil
}

// Exist check if the function exists
func (f *Function) Exist() (bool, error) {
	err := f.Load()
	var httpErr restclient.HttpError
	if errors.As(err, &httpErr) {
		if httpErr.Status == "404 Not Found" {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Name get function name
func (f *Function) Name() string {
	return f.Model.Name
}

// SetName set function name
func (f *Function) SetName(name string) {
	f.Model.Name = name
}

// Owner get function owner
func (f *Function) Owner() string {
	return f.Model.Owner
}

// CreationTime get function creation time
func (f *Function) CreationTime() time.Time {
	date, _ := common.ParseRFC1123Date(f.Model.CreationTime)
	return date
}

// ClassPath get function class path
func (f *Function) ClassPath() string {
	return f.Model.ClassPath
}

// SetClassPath set function class path
func (f *Function) SetClassPath(classPath string) {
	f.Model.ClassPath = classPath
}

// Resources get function resources
func (f *Function) Resources() []string {
	if f.Model.Resources == nil {
		return nil
	}

	var resources []string
	for i := range f.Model.Resources {
		resources = append(resources, f.Model.Resources[i].ResourceName)
	}
	return resources
}

// SetResources set function resources
func (f *Function) SetResources(resources []string) {
	if resources == nil {
		return
	}

	var fResources []FResource
	for i := range resources {
		fResources = append(fResources, FResource{ResourceName: resources[i]})
	}
	f.Model.Resources = fResources
}

// Project get function project
func (f *Function) Project() string {
	return f.ProjectName
}

// SchemaName get function schema name
func (f *Function) SchemaName() string {
	return f.Model.SchemaName
}

// IsSQLFunction get whether function is sql function
func (f *Function) IsSQLFunction() bool {
	return f.Model.IsSQLFunction
}

// SQLDefinitionText get function sql definition text
func (f *Function) SQLDefinitionText() string {
	return f.Model.SQLDefinitionText
}

// IsEmbeddedFunction get whether function is embedded function
func (f *Function) IsEmbeddedFunction() bool {
	return f.Model.IsEmbeddedFunction
}

// ProgramLanguage get function program language
func (f *Function) ProgramLanguage() string {
	return f.Model.ProgramLanguage
}

// Code get function code
func (f *Function) Code() string {
	return f.Model.Code
}

// FunctionBuilder is a builder for Function
type FunctionBuilder struct {
	schemaName         string
	name               string
	owner              string
	creationTime       string
	classPath          string
	resources          []FResource
	isSQLFunction      bool
	sqlDefinitionText  string
	isEmbeddedFunction bool
	programLanguage    string
	code               string
	fileName           string
	projectName        string
	odpsIns            *Odps
}

// NewFunctionBuilder create a new FunctionBuilder
func NewFunctionBuilder() *FunctionBuilder {
	return &FunctionBuilder{}
}

// Name set function name
func (fb *FunctionBuilder) Name(name string) *FunctionBuilder {
	fb.name = name
	return fb
}

// SchemaName set function schema name
func (fb *FunctionBuilder) SchemaName(schemaName string) *FunctionBuilder {
	fb.schemaName = schemaName
	return fb
}

// Owner set function owner
func (fb *FunctionBuilder) Owner(owner string) *FunctionBuilder {
	fb.owner = owner
	return fb
}

// CreationTime set function creation time
func (fb *FunctionBuilder) CreationTime(creationTime common.GMTTime) *FunctionBuilder {
	fb.creationTime = creationTime.String()
	return fb
}

// ClassPath set function class path
func (fb *FunctionBuilder) ClassPath(classPath string) *FunctionBuilder {
	fb.classPath = classPath
	return fb
}

// Resources set function resources
func (fb *FunctionBuilder) Resources(resources []string) *FunctionBuilder {
	if resources == nil {
		return fb
	}
	var fResources []FResource
	for i := range resources {
		fResources = append(fResources, FResource{ResourceName: resources[i]})
	}
	fb.resources = fResources
	return fb
}

// IsSQLFunction set whether function is sql function
func (fb *FunctionBuilder) IsSQLFunction(isSQLFunction bool) *FunctionBuilder {
	fb.isSQLFunction = isSQLFunction
	return fb
}

// SQLDefinitionText set function sql definition text
func (fb *FunctionBuilder) SQLDefinitionText(sqlDefinitionText string) *FunctionBuilder {
	fb.sqlDefinitionText = sqlDefinitionText
	return fb
}

// IsEmbeddedFunction set whether function is embedded function
func (fb *FunctionBuilder) IsEmbeddedFunction(isEmbeddedFunction bool) *FunctionBuilder {
	fb.isEmbeddedFunction = isEmbeddedFunction
	return fb
}

// ProgramLanguage set function program language
func (fb *FunctionBuilder) ProgramLanguage(programLanguage string) *FunctionBuilder {
	fb.programLanguage = programLanguage
	return fb
}

// Code set function code
func (fb *FunctionBuilder) Code(code string) *FunctionBuilder {
	fb.code = code
	return fb
}

// FileName set function file name
func (fb *FunctionBuilder) FileName(fileName string) *FunctionBuilder {
	fb.fileName = fileName
	return fb
}

// ProjectName set function project name
func (fb *FunctionBuilder) ProjectName(projectName string) *FunctionBuilder {
	fb.projectName = projectName
	return fb
}

// Odps set function odps
func (fb *FunctionBuilder) Odps(odpsIns *Odps) *FunctionBuilder {
	fb.odpsIns = odpsIns
	return fb
}

// Build build a Function
func (fb *FunctionBuilder) Build() Function {
	return Function{
		ProjectName: fb.projectName,
		OdpsIns:     fb.odpsIns,
		Model: functionModel{
			SchemaName:         fb.schemaName,
			Name:               fb.name,
			Owner:              fb.owner,
			CreationTime:       fb.creationTime,
			ClassPath:          fb.classPath,
			Resources:          fb.resources,
			IsSQLFunction:      fb.isSQLFunction,
			SQLDefinitionText:  fb.sqlDefinitionText,
			IsEmbeddedFunction: fb.isEmbeddedFunction,
			ProgramLanguage:    fb.programLanguage,
			Code:               fb.code,
			FileName:           fb.fileName,
		},
	}
}
